package fr.hgwood.todomvckafka;

import lombok.Value;

import java.time.ZonedDateTime;
import java.util.UUID;

import static java.time.ZonedDateTime.now;
import static java.util.UUID.randomUUID;

@Value
public class Transaction {
    private final Id id;
    private final ZonedDateTime timestamp;

    public static Transaction createTransaction() {
        return new Transaction(new Id(randomUUID()), now());
    }

    @Value
    public static class Id {
        private final UUID value;
    }
}
