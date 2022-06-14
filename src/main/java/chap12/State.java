package chap12;

public enum State {
    Terminal,

    Matched,

    S2(new Transition("failed", Matched), new Transition("success", Terminal)),

    S1(new Transition("failed", S2), new Transition("success", Terminal)),

    Initial(new Transition("failed", S1), new Transition("success", Terminal));

    private Transition[] transitions;

    State(Transition... transitions) {
        this.transitions = transitions;
    }

    public State transition(String eventType) {
        for(Transition t: transitions) {
            if(t.getEventType().equals(eventType)) {
                return t.getTargetState();
            }
        }
        return Initial;
    }

}
