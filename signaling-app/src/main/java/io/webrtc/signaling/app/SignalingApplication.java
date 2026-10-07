package io.webrtc.signaling.app;

import io.webrtc.signaling.app.config.SignalingProperties;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.context.event.ApplicationFailedEvent;
import org.springframework.boot.context.event.ApplicationPreparedEvent;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

@SpringBootApplication(exclude = {
    org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration.class,
    org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration.class,
    org.springframework.boot.autoconfigure.flyway.FlywayAutoConfiguration.class
})
@EnableConfigurationProperties(SignalingProperties.class)
public class SignalingApplication {
    public enum Plane { GATEWAY, ACTOR, CONTROL }

    public static void main(String[] args) {
        application().run(args);
    }

    public static SpringApplication application(){
        var application=new SpringApplication(SignalingApplication.class);
        var cleanup=new NativeStartupCleanup();
        application.addInitializers(context->{
            context.getBeanFactory().registerSingleton(NativeStartupCleanup.OWNER_BEAN,new NativeStartupCleanup.Owner(cleanup));
            context.getBeanFactory().addBeanPostProcessor(cleanup);
        });
        application.addListeners((ApplicationListener<ApplicationPreparedEvent>)event->cleanup.capture(event.getApplicationContext()));
        application.addListeners((ApplicationListener<ApplicationFailedEvent>)event->{
            cleanup.failed(event.getApplicationContext(),event.getException());
            System.err.println(SafeStartupFailure.render(event.getException()));
        });
        return application;
    }

    @Bean ApplicationRunner requireNativePlane(Plane plane,ApplicationContext context){
        return args->{
            try{NativeRuntimeStartup.requireInstalled(plane,context);}
            catch(RuntimeException failed){NativeStartupCleanup.cleanup(context,failed);throw failed;}
        };
    }

    @Bean Plane deploymentPlane(Environment environment) {
        List<String> planes = Arrays.stream(environment.getActiveProfiles())
            .filter(p -> List.of("gateway", "actor", "control").contains(p)).toList();
        if (planes.size() != 1) throw new IllegalArgumentException("Select exactly one deployment plane: gateway, actor or control");
        return Plane.valueOf(planes.getFirst().toUpperCase(Locale.ROOT));
    }
}
