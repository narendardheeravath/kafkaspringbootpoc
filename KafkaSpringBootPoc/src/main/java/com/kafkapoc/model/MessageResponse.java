package com.kafkapoc.model;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Schema(description = "Kafka publish result")
public class MessageResponse {

    @Schema(description = "Result status", example = "SENT")
    private String status;

    @Schema(description = "Topic the message(s) were published to", example = "topic-orders")
    private String topic;

    @Schema(description = "Number of messages published", example = "1")
    private int messageCount;

    @Schema(description = "Consumer pattern demonstrated by this topic")
    private String scenario;

    @Schema(description = "Consumer groups that will receive the message(s)")
    private List<String> consumerGroups;

    @Schema(description = "ISO-8601 publish timestamp")
    private String sentAt;

    public static MessageResponse success(String topic, String scenario, List<String> groups, int count) {
        return MessageResponse.builder()
                .status("SENT")
                .topic(topic)
                .scenario(scenario)
                .consumerGroups(groups)
                .messageCount(count)
                .sentAt(Instant.now().toString())
                .build();
    }
}
