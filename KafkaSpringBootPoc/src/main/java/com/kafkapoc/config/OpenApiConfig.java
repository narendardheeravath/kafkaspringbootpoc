package com.kafkapoc.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.tags.Tag;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI kafkaPocOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("Kafka Spring Boot POC API")
                        .version("1.0.0")
                        .description("""
                                **Kafka consumer pattern demonstrations** — port 8096
                                
                                | Scenario | Topic | Pattern |
                                |---|---|---|
                                | 1 | `topic-orders` (3 partitions) | Single Subscriber — one group, one thread |
                                | 2 | `topic-notifications` (3 partitions) | Multiple Subscribers — 3 groups each get every message (broadcast) |
                                | 3 | `topic-inventory` (6 partitions) | Multiple Groups — 2 groups independently consume all events (CQRS) |
                                | 4 | `topic-audit` (6 partitions) | Competing Consumers — same group, concurrency=3, messages load-balanced |
                                | 5 | `topic-payment` (3 partitions) | Batch Consumer — messages collected and processed in batches |
                                
                                **Kafka UI Dashboard:** http://localhost:8090
                                """)
                        .contact(new Contact()
                                .name("KafkaSpringBootPoc")
                                .email("dev@example.com")));
    }
}
