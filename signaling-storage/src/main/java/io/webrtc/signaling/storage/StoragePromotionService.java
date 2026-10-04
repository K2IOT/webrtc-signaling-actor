package io.webrtc.signaling.storage;

import java.io.*;
import java.security.*;
import java.sql.Timestamp;
import java.time.*;
import java.util.*;
import java.util.function.Predicate;

/** Administrative HA transition, separate from PITR. Serving database roles must not own this capability. */
public final class StoragePromotionService {
    /** The enrolled HA source attests both complete physical fencing and preservation of acknowledged WAL. */
    public record Permit(String cell,long previousEpoch,long newEpoch,UUID operation,Instant checkedAt,
            Instant validUntil,String oldPrimaryFenceReceiptSha256,String acknowledgedWalReceiptSha256,
            String keyId,String signedEvidence) {
        public Permit {
            if(cell==null||!cell.matches("[a-z][a-z0-9-]{0,23}")||previousEpoch<1||newEpoch<=previousEpoch
                    ||oldPrimaryFenceReceiptSha256==null||!oldPrimaryFenceReceiptSha256.matches("[a-f0-9]{64}")
                    ||acknowledgedWalReceiptSha256==null||!acknowledgedWalReceiptSha256.matches("[a-f0-9]{64}")
                    ||keyId==null||!keyId.matches("[A-Za-z0-9_.-]{1,128}")||signedEvidence==null||signedEvidence.length()>128)
                throw new IllegalArgumentException("Invalid promotion permit");
            Objects.requireNonNull(operation);Objects.requireNonNull(checkedAt);Objects.requireNonNull(validUntil);
            if(!validUntil.isAfter(checkedAt)||Duration.between(checkedAt,validUntil).compareTo(Duration.ofSeconds(5))>0)
                throw new IllegalArgumentException("Invalid promotion validity");
        }
        @Override public String toString(){return "PromotionPermit[cell="+cell+", epoch="+newEpoch+", operation="+operation+"]";}
    }
    public record EnrolledSource(String keyId,String cell,PublicKey key) {
        public EnrolledSource {Objects.requireNonNull(keyId);Objects.requireNonNull(cell);Objects.requireNonNull(key);
            if(!Set.of("EdDSA","Ed25519").contains(key.getAlgorithm()))throw new IllegalArgumentException("Ed25519 HA source required");}
    }
    public static byte[] signingBytes(Permit p) {
        try(var bytes=new ByteArrayOutputStream();var out=new DataOutputStream(bytes)) {
            out.writeUTF("signaling-ha-promotion-v1");out.writeUTF(p.cell());out.writeLong(p.previousEpoch());out.writeLong(p.newEpoch());
            out.writeLong(p.operation().getMostSignificantBits());out.writeLong(p.operation().getLeastSignificantBits());
            out.writeUTF(p.checkedAt().toString());out.writeUTF(p.validUntil().toString());
            out.writeUTF(p.oldPrimaryFenceReceiptSha256());out.writeUTF(p.acknowledgedWalReceiptSha256());out.writeUTF(p.keyId());
            out.flush();return bytes.toByteArray();
        }catch(IOException impossible){throw new IllegalStateException(impossible);}
    }
    public static Predicate<Permit> enrolledVerifier(List<EnrolledSource> sources) {
        if(sources.size()>256)throw new IllegalArgumentException("HA trust set exceeds bound");
        var enrolled=new HashMap<String,EnrolledSource>();for(var source:sources)if(enrolled.putIfAbsent(source.keyId(),source)!=null)throw new IllegalArgumentException("Duplicate HA source");
        var trust=Map.copyOf(enrolled);
        return p->{try{var source=trust.get(p.keyId());if(source==null||!source.cell().equals(p.cell()))return false;
            var signature=Signature.getInstance("Ed25519");signature.initVerify(source.key());signature.update(signingBytes(p));
            byte[] value=Base64.getUrlDecoder().decode(p.signedEvidence());return value.length==64&&signature.verify(value);
        }catch(GeneralSecurityException|IllegalArgumentException invalid){return false;}};
    }
    private final SqlTransactions sql;private final String cell;private final Predicate<Permit> admission;
    public StoragePromotionService(SqlTransactions administrativeSql,String cell,Predicate<Permit> admittedHaSource) {
        sql=Objects.requireNonNull(administrativeSql);this.cell=Objects.requireNonNull(cell);admission=Objects.requireNonNull(admittedHaSource);
    }
    public DbOperation<Long> promote(Permit p) {
        if(p==null||!cell.equals(p.cell())||!admission.test(p))throw new AuthoritySql.FencedException();
        return sql.submitTracked(DbClass.RECOVERY,Duration.ofSeconds(2),c->{
            AuthoritySql.cellBarrier(c,true);
            try(var q=c.prepareStatement("SELECT ?::timestamptz+interval '250 milliseconds'>=clock_timestamp() AND ?::timestamptz<=clock_timestamp()+interval '250 milliseconds' AND ?::timestamptz>clock_timestamp()+interval '250 milliseconds'")) {
                q.setTimestamp(1,Timestamp.from(p.checkedAt().plusSeconds(5)));q.setTimestamp(2,Timestamp.from(p.checkedAt()));q.setTimestamp(3,Timestamp.from(p.validUntil()));
                try(var r=q.executeQuery()){r.next();if(!r.getBoolean(1))throw new AuthoritySql.FencedException();}
            }
            long current;try(var q=c.createStatement();var r=q.executeQuery("SELECT cell_id,storage_epoch,status,ownership_mode,ownership_schema_version FROM cell_authority WHERE singleton_id=1")) {
                if(!r.next()||!cell.equals(r.getString(1))||!"ACTIVE".equals(r.getString(3))||!"GROUPED".equals(r.getString(4))||r.getLong(5)!=1)throw new AuthoritySql.FencedException();current=r.getLong(2);
            }
            if(current==p.newEpoch()) {
                try(var q=c.prepareStatement("SELECT previous_epoch,operation_id,fence_receipt_sha256,wal_receipt_sha256 FROM promotion_epoch_journal WHERE storage_epoch=?")) {
                    q.setLong(1,current);try(var r=q.executeQuery()){if(!r.next()||r.getLong(1)!=p.previousEpoch()||!p.operation().equals(r.getObject(2,UUID.class))||!p.oldPrimaryFenceReceiptSha256().equals(r.getString(3))||!p.acknowledgedWalReceiptSha256().equals(r.getString(4)))throw new AuthoritySql.FencedException();}
                }return current;
            }
            if(current!=p.previousEpoch())throw new AuthoritySql.FencedException();
            try(var q=c.prepareStatement("INSERT INTO promotion_epoch_journal(storage_epoch,previous_epoch,operation_id,fence_receipt_sha256,wal_receipt_sha256,source_key_id) VALUES(?,?,?,?,?,?)")) {
                q.setLong(1,p.newEpoch());q.setLong(2,p.previousEpoch());q.setObject(3,p.operation());q.setString(4,p.oldPrimaryFenceReceiptSha256());q.setString(5,p.acknowledgedWalReceiptSha256());q.setString(6,p.keyId());q.executeUpdate();
            }
            try(var q=c.prepareStatement("UPDATE cell_authority SET storage_epoch=? WHERE singleton_id=1 AND storage_epoch=? AND status='ACTIVE'")) {
                q.setLong(1,p.newEpoch());q.setLong(2,p.previousEpoch());if(q.executeUpdate()!=1)throw new AuthoritySql.FencedException();
            }return p.newEpoch();
        });
    }
}
