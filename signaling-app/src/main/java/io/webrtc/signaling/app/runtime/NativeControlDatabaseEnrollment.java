package io.webrtc.signaling.app.runtime;

import io.webrtc.signaling.storage.*;
import java.util.*;
import java.util.function.BooleanSupplier;

/** Explicit regional directory/local cell databases, fixed quotas and independent local authority. */
public record NativeControlDatabaseEnrollment(Database regional,Database local,String cell,long storageEpoch,
        BooleanSupplier admittedRegionalWriter) {
    public NativeControlDatabaseEnrollment {
        Objects.requireNonNull(regional);Objects.requireNonNull(local);Objects.requireNonNull(admittedRegionalWriter);
        if(cell==null||!cell.matches("[a-z][a-z0-9-]{0,23}")||storageEpoch<1)throw new IllegalArgumentException("Invalid control database authority");
    }
    public record Database(String jdbcUrl,String username,String password,Map<DbClass,Integer> quotas,int safetyMax,int controlMax) {
        public Database {
            if(jdbcUrl==null||!jdbcUrl.startsWith("jdbc:postgresql:")||username==null||username.isBlank()||password==null)
                throw new IllegalArgumentException("Explicit PostgreSQL database credentials required");
            quotas=Map.copyOf(quotas);var admission=new DbAdmission(quotas);
            if(safetyMax<1||controlMax<1||admission.poolCapacity(true)>safetyMax||admission.poolCapacity(false)>controlMax
                    ||!quotas.containsKey(DbClass.RECOVERY)||!quotas.containsKey(DbClass.CRITICAL))
                throw new IllegalArgumentException("Control database quotas exceed fixed pool budget or omit directory work");
        }
        @Override public String toString(){return "ControlDatabase[credentials=redacted]";}
    }
}
