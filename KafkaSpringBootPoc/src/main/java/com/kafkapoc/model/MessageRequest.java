package com.kafkapoc.model;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Schema(description = "Kafka message request")
public class MessageRequest {

    @Schema(description = "Optional message key — controls which partition receives the message", example = "order-001")
    private String key;

    @NotBlank(message = "message must not be blank")
    @Schema(description = "Message payload to publish", example = "Order #001: 2x Product ABC, customer: John Doe")
    private String message;
}
