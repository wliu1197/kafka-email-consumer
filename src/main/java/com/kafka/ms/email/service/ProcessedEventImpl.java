package com.kafka.ms.email.service;

import com.kafka.ms.email.model.ProcessedEventDetails;
import com.kafka.ms.email.repository.ProcessedEventRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Qualifier("ProcessedEventService")
public class ProcessedEventImpl implements ProcessedEventAgent{
    private ProcessedEventRepository processedEventRepository;
    @Autowired
    public ProcessedEventImpl(ProcessedEventRepository processedEventRepository) {
        this.processedEventRepository = processedEventRepository;
    }

    @Override
    public ProcessedEventDetails saveEvent(ProcessedEventDetails processedEventDetails){
        return processedEventRepository.save(processedEventDetails);
    }

    @Override
    public ProcessedEventDetails findByMessageId(String messageId){
        return processedEventRepository.findByMessageId(messageId);
    }
}
