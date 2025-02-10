package com.kafka.ms.email.service;

import com.kafka.ms.email.model.ProcessedEventDetails;

import java.util.List;

public interface ProcessedEventAgent {
    ProcessedEventDetails saveEvent(ProcessedEventDetails processedEventDetails);
    ProcessedEventDetails findByMessageId(String messageId);
}
