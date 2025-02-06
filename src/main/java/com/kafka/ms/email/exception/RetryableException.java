package com.kafka.ms.email.exception;

public class RetryableException extends RuntimeException{
    public RetryableException(String message) {
        super(message);
    }
}
