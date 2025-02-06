package com.kafka.ms.email.exception;

public class NotRetryableException extends RuntimeException{
    public NotRetryableException(String message) {
        super(message);
    }
}
