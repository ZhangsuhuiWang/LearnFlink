package chap12;

import java.io.Serializable;

public class Transition implements Serializable {
    private static final long serialVersionUID = 1l;

    private final String eventType;

    private final State targetState;

    public Transition(String eventType, State targetState) {
        this.eventType = eventType;
        this.targetState = targetState;
    }

    public String getEventType() {
        return eventType;
    }

    public State getTargetState() {
        return targetState;
    }
}
