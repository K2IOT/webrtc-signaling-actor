package io.webrtc.signaling.storage;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.webrtc.signaling.observability.OperationObservation;
import io.webrtc.signaling.observability.SignalingMetrics.Operation;
import io.webrtc.signaling.observability.SignalingMetrics.Outcome;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;

import java.sql.Connection;
import java.util.Map;
import java.util.Objects;

/**
 * Exactly two fixed pools and matching JPA transaction managers. No default datasource.
 */
public final class DbPools implements AutoCloseable {
    private final Binding safety, control;
    private boolean observed;

    public DbPools(
            String url,
            String user,
            String password,
            DbAdmission admission,
            int safetyMax,
            int controlMax) {
        if (safetyMax < 1
                || controlMax < 1
                || admission.poolCapacity(true) > safetyMax
                || admission.poolCapacity(false) > controlMax)
            throw new IllegalArgumentException("DB quotas exceed fixed connection budget");
        safety = create("safety", url, user, password, safetyMax);
        try {
            control = create("control", url, user, password, controlMax);
        } catch (RuntimeException e) {
            close(safety);
            throw e;
        }
    }

    private static Binding create(String name, String url, String user, String password, int max) {
        var cfg = new HikariConfig();
        cfg.setPoolName("signaling-" + name);
        cfg.setJdbcUrl(url);
        cfg.setUsername(user);
        cfg.setPassword(password);
        cfg.setMaximumPoolSize(max);
        cfg.setMinimumIdle(0);
        cfg.setConnectionTimeout(500);
        cfg.setValidationTimeout(250);
        cfg.setInitializationFailTimeout(-1);
        cfg.addDataSourceProperty("connectTimeout", 2);
        cfg.addDataSourceProperty("socketTimeout", 3);
        cfg.addDataSourceProperty("cancelSignalTimeout", 1);
        cfg.addDataSourceProperty("ApplicationName", "signaling-" + name);
        cfg.setAutoCommit(false);
        var ds = new HikariDataSource(cfg);
        var factory = new LocalContainerEntityManagerFactoryBean();
        try {
            factory.setPersistenceUnitName("signaling-" + name);
            factory.setDataSource(ds);
            factory.setPackagesToScan("io.webrtc.signaling.storage");
            factory.setJpaVendorAdapter(new HibernateJpaVendorAdapter());
            factory.setJpaPropertyMap(
                    Map.of(
                            "hibernate.hbm2ddl.auto",
                            "none",
                            "hibernate.cache.use_second_level_cache",
                            false,
                            "hibernate.cache.use_query_cache",
                            false,
                            "hibernate.jdbc.time_zone",
                            "UTC"));
            factory.afterPropertiesSet();
            var manager = new JpaTransactionManager(Objects.requireNonNull(factory.getObject()));
            manager.setDataSource(ds);
            manager.afterPropertiesSet();
            return new Binding(ds, factory, manager);
        } catch (RuntimeException e) {
            ds.close();
            throw e;
        }
    }

    private static void close(Binding b) {
        b.factory().destroy();
        b.dataSource().close();
    }

    JpaTransactionManager manager(DbClass clazz) {
        return binding(clazz).manager();
    }

    public Connection currentConnection(DbClass clazz) {
        if (!org.springframework.transaction.support.TransactionSynchronizationManager
                .isActualTransactionActive()) throw new IllegalStateException("No admitted transaction");
        return DataSourceUtils.getConnection(binding(clazz).dataSource());
    }

    private Binding binding(DbClass clazz) {
        return clazz.safety() ? safety : control;
    }

    public synchronized void observe(OperationObservation observer) {
        if (observed) return;
        observed = true;
        for (var binding : java.util.List.of(safety, control)) {
            var operation =
                    binding == safety ? Operation.DB_CONNECTION_SAFETY : Operation.DB_CONNECTION_CONTROL;
            binding
                    .dataSource()
                    .setMetricsTrackerFactory(
                            (poolName, stats) ->
                                    new com.zaxxer.hikari.metrics.IMetricsTracker() {
                                        public void recordConnectionAcquiredNanos(long elapsedNanos) {
                                            var receipt = OperationObservation.safeStart(observer, operation);
                                            receipt.poolWait(elapsedNanos);
                                            receipt.logical(Outcome.READ);
                                            receipt.physical(true);
                                        }
                                    });
        }
    }

    public PoolObservation observation(boolean safetyPool) {
        var source = (safetyPool ? safety : control).dataSource();
        var bean = source.getHikariPoolMXBean();
        return bean == null || source.isClosed()
                ? null
                : new PoolObservation(bean.getActiveConnections(), bean.getThreadsAwaitingConnection());
    }

    public boolean closed() {
        return control.dataSource().isClosed() && safety.dataSource().isClosed();
    }

    @Override
    public void close() {
        close(control);
        close(safety);
    }

    private record Binding(
            HikariDataSource dataSource,
            LocalContainerEntityManagerFactoryBean factory,
            JpaTransactionManager manager) {
    }

    public record PoolObservation(int active, int pending) {
    }
}
