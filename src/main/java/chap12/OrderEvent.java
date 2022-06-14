package chap12;

public class OrderEvent {
    public String userId;
    public String orderId;
    public String eventType;
    public Long timeStamp;

    public OrderEvent(String userId, String orderId, String eventType, Long timeStamp) {
        this.userId = userId;
        this.orderId = orderId;
        this.eventType = eventType;
        this.timeStamp = timeStamp;
    }

    public OrderEvent() {}

    @Override
    public String toString() {
        return "OrderEvent{" +
                "userId='" + userId + '\'' +
                ", orderId='" + orderId + '\'' +
                ", eventType='" + eventType + '\'' +
                ", timeStamp=" + timeStamp +
                '}';
    }
}
