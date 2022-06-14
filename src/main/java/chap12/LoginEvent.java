package chap12;

public class LoginEvent {
    public String userId;
    public String ipAddress;
    public String eventType;
    public Long timeStamp;

    public LoginEvent(String userId, String ipAddress, String eventType, Long timeStamp) {
        this.userId = userId;
        this.ipAddress = ipAddress;
        this.eventType = eventType;
        this.timeStamp = timeStamp;
    }

    public LoginEvent() {}

    @Override
    public String toString() {
        return "LoginEvent{" +
                "userId='" + userId + '\'' +
                ", ipAddress='" + ipAddress + '\'' +
                ", eventType='" + eventType + '\'' +
                ", timeStamp=" + timeStamp +
                '}';
    }
}
