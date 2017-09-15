package fr.hgwood.todomvckafka;

import fr.hgwood.todomvckafka.facts.Fact;
import io.vavr.collection.Set;
import lombok.Value;

@Value
public class Transaction {

    private final Set<Fact> facts;

}
