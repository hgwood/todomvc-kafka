package fr.hgwood.todomvckafka;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class Action {
    private final ActionType type;
    private final String id;
    private final String text;
}
