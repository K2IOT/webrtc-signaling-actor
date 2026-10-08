package io.webrtc.signaling.app.runtime;

import io.webrtc.signaling.storage.SqlTransactions;
import io.webrtc.signaling.control.PostgresDirectoryRepository;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.*;
import org.springframework.context.annotation.*;

/** Two separately enrolled native authorities. No default database, migration or writer predicate. */
@AutoConfiguration(before=NativeControlBusinessConfiguration.class)
@Profile("control")
@ConditionalOnBean(NativeControlDatabaseEnrollment.class)
public class NativeControlDatabaseConfiguration {
    @Bean(destroyMethod="") NativeControlDatabaseResources nativeControlDatabaseResources(NativeControlDatabaseEnrollment inputs){return new NativeControlDatabaseResources(inputs);}
    @Bean SqlTransactions nativeControlRegionalSql(NativeControlDatabaseResources resources){return resources.regional();}
    @Bean SqlTransactions nativeControlLocalSql(NativeControlDatabaseResources resources){return resources.local();}
    @Bean @ConditionalOnMissingBean(PostgresDirectoryRepository.class)
    PostgresDirectoryRepository nativeControlDirectoryRepository(NativeControlDatabaseEnrollment inputs,
            @Qualifier("nativeControlRegionalSql") SqlTransactions regional,@Qualifier("nativeControlLocalSql") SqlTransactions local){
        return new PostgresDirectoryRepository(regional,local,inputs.cell(),inputs.storageEpoch(),inputs.admittedRegionalWriter());
    }
}
