package fr.hgwood.todomvckafka2.facts;

import io.vavr.collection.Set;
import lombok.Value;

@Value
public class Transaction {
    private final Set<Fact> facts;
}
