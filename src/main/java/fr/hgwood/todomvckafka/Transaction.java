package fr.hgwood.todomvckafka;

import io.vavr.collection.Set;
import lombok.Value;

import java.time.ZonedDateTime;
import java.util.UUID;

import static java.time.ZonedDateTime.now;
import static java.util.UUID.randomUUID;

@Value
public class Transaction {

    private final Set<Fact> facts;

}
