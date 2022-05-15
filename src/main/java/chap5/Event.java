package chap5;

import java.sql.Timestamp;

public class Event {
    public String users;
    public String urls;
    public Long timestamp;

    public Event() {

    }

    public Event(String users, String urls, Long timestamp) {
        this.users = users;
        this.urls = urls;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "chap5.Event{" +
                "users='" + users + '\'' +
                ", urls='" + urls + '\'' +
                ", timestamp=" + new Timestamp(timestamp) +
                '}';
    }
}
