package io.webrtc.signaling.control;

import io.webrtc.signaling.storage.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.function.BooleanSupplier;

/** Regional primary hints and independent native cell authority, through fixed admitted pools. */
public final class PostgresDirectoryRepository implements DirectoryRepository {
    private static final Duration BUDGET=Duration.ofSeconds(2);
    private final SqlTransactions regional,local;
    private final String cell;
    private final long storageEpoch;
    private final BooleanSupplier admittedRegionalWriter;
    public PostgresDirectoryRepository(SqlTransactions regional,SqlTransactions local,String cell,long storageEpoch,BooleanSupplier admittedRegionalWriter){
        this.regional=Objects.requireNonNull(regional);this.local=Objects.requireNonNull(local);
        if(cell==null||!cell.matches("[a-z][a-z0-9-]{0,23}")||storageEpoch<1)throw new IllegalArgumentException("Invalid local authority");
        this.cell=cell;this.storageEpoch=storageEpoch;this.admittedRegionalWriter=Objects.requireNonNull(admittedRegionalWriter);
    }
    private static void bucket(int bucket){if(bucket<0||bucket>=16384)throw new IllegalArgumentException("Invalid directory bucket");}
    private void writer(){if(!admittedRegionalWriter.getAsBoolean())throw new AuthoritySql.FencedException();}
    /** Both independent authorities must be enrolled in the process's physical drain. */
    public List<SqlTransactions> transactionOwners(){return regional==local?List.of(regional):List.of(regional,local);}
    @Override public CompletionStage<Optional<HomeRoute>> read(int bucket){
        bucket(bucket);
        return regional.submit(DbClass.RECOVERY,BUDGET,c->{writer();
            try(var q=c.prepareStatement("SELECT cell_id,directory_epoch,wss_url FROM directory_bucket WHERE bucket_id=?")){
                q.setInt(1,bucket);try(var r=q.executeQuery()){
                    Optional<HomeRoute> route=r.next()?Optional.of(new HomeRoute(bucket,r.getString(1),r.getLong(2),r.getString(3))):Optional.empty();writer();return route;
                }
            }
        });
    }
    @Override public CompletionStage<Boolean> compareAndPublish(HomeRoute previous,HomeRoute next){
        Objects.requireNonNull(previous);Objects.requireNonNull(next);
        if(previous.bucket()!=next.bucket()||next.epoch()<=previous.epoch()||next.wssUrl().length()>512)throw new IllegalArgumentException("Publication must advance the same bucket");
        return regional.submit(DbClass.CRITICAL,BUDGET,c->{writer();
            try(var q=c.prepareStatement("UPDATE directory_bucket SET cell_id=?,directory_epoch=?,wss_url=?,updated_at=clock_timestamp() WHERE bucket_id=? AND cell_id=? AND directory_epoch=? AND wss_url=?")){
                q.setString(1,next.cell());q.setLong(2,next.epoch());q.setString(3,next.wssUrl());q.setInt(4,previous.bucket());q.setString(5,previous.cell());q.setLong(6,previous.epoch());q.setString(7,previous.wssUrl());
                boolean published=q.executeUpdate()==1;writer();return published;
            }
        });
    }
    /** Directory availability never leases or authorizes a native mutation. */
    @Override public CompletionStage<Boolean> localActive(int bucket,String expectedCell,long epoch){
        bucket(bucket);if(!cell.equals(expectedCell)||epoch<1)return java.util.concurrent.CompletableFuture.completedFuture(false);
        return local.submit(DbClass.RECOVERY,BUDGET,c->{
            AuthoritySql.cellBarrier(c,false);AuthoritySql.bucketBarrier(c,bucket,false);
            try{AuthoritySql.validateCell(c,cell,storageEpoch);AuthoritySql.validateBuckets(c,Map.of(bucket,epoch));return true;}
            catch(AuthoritySql.FencedException stale){return false;}
        });
    }
}
