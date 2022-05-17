package chap06;

import org.apache.commons.net.ntp.TimeStamp;

public class UrlViewCount {
    public String url;
    public Long count;
    public Long windowStart;
    public Long windowEnd;

    public UrlViewCount() {

    }

    public UrlViewCount(String url, Long count, Long windowStart, Long windowEnd) {
        this.url = url;
        this.count = count;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
    }

    @Override
    public String toString() {
        return "UrlViewCount{" +
                "url='" + url + '\'' +
                ", count=" + count +
                ", windowStart=" + new TimeStamp(windowStart) +
                ", windowEnd=" + new TimeStamp(windowEnd) +
                '}';
    }
}
