package fr.hgwood.todomvckafka;

import fr.hgwood.todomvckafka.facts.FactRequest;
import lombok.Value;

@Value
public class FactRequestTransaction {
    private final Transaction<FactRequest> transaction;
}
