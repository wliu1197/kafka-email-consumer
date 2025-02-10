package com.kafka.ms.email.repository;

import com.kafka.ms.email.model.ProcessedEventDetails;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface ProcessedEventRepository extends JpaRepository<ProcessedEventDetails, Integer> {
    ProcessedEventDetails findByMessageId(String messageId);
}
