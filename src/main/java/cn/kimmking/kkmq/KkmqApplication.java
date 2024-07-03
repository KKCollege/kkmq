package cn.kimmking.kkmq;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;

@SpringBootApplication
public class KkmqApplication {

    public static void main(String[] args) {
        SpringApplication.run(KkmqApplication.class, args);
    }

    @Autowired
    ApplicationContext context;

    @Bean
    ApplicationRunner runner() {
        return args -> {
            RequestMappingHandlerMapping bean = context.getBean(RequestMappingHandlerMapping.class);
            bean.getHandlerMethods().forEach((k, v) -> {
                System.out.println(k.toString());
            });
        };
    }

}
