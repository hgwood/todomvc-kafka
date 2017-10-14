package fr.hgwood.todomvckafka;

import fr.hgwood.todomvckafka.facts.Fact;
import lombok.Value;

@Value
public class FactTransaction {
    private final Transaction<Fact> transaction;
}
