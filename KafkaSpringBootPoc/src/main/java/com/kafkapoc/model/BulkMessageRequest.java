package com.kafkapoc.model;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Size;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Schema(description = "Bulk Kafka message request — sends multiple messages at once to demonstrate load balancing and batch processing")
public class BulkMessageRequest {

    @NotEmpty(message = "messages list must not be empty")
    @Size(min = 1, max = 50, message = "Must send between 1 and 50 messages at once")
    @Schema(description = "List of message payloads to send",
            example = "[\"Payment txn-001: $150.00\", \"Payment txn-002: $89.99\", \"Payment txn-003: $220.00\"]")
    private List<String> messages;

    @Schema(description = "Optional key prefix (appended with index, e.g. audit-0, audit-1)", example = "audit")
    private String keyPrefix;
}
