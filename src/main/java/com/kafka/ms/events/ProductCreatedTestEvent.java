package com.kafka.ms.events;

public class ProductCreatedTestEvent {
    private String productId;
    private String title;

    public ProductCreatedTestEvent() {
    }

    public ProductCreatedTestEvent(String productId, String title) {
        this.productId = productId;
        this.title = title;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }
}
