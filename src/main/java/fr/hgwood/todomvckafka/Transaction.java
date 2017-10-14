package fr.hgwood.todomvckafka;

import fr.hgwood.todomvckafka.facts.Fact;
import io.vavr.collection.Set;
import lombok.Value;

@Value
public class Transaction<T> {

    private final Set<T> facts;

}
