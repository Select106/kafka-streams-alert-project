package org.example.model;

public class AlertMessage {
    private String productId;
    private String productName;
    private long windowStart;
    private long windowEnd;
    private double totalRevenue;

    public AlertMessage() {
    }

    public AlertMessage(String productId, String productName, long windowStart, long windowEnd, double totalRevenue) {
        this.productId = productId;
        this.productName = productName;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.totalRevenue = totalRevenue;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public long getWindowStart() {
        return windowStart;
    }

    public void setWindowStart(long windowStart) {
        this.windowStart = windowStart;
    }

    public long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public double getTotalRevenue() {
        return totalRevenue;
    }

    public void setTotalRevenue(double totalRevenue) {
        this.totalRevenue = totalRevenue;
    }
}
