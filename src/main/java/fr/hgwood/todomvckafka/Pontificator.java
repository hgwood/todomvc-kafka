package fr.hgwood.todomvckafka;

import fr.hgwood.todomvckafka.actions.Action;
import fr.hgwood.todomvckafka.support.kafkastreams.TopicInfo;
import fr.hgwood.todomvckafka.support.kafkastreams.Topology;
import lombok.AllArgsConstructor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.function.Supplier;

@AllArgsConstructor
public class Pontificator implements Topology {

    private final TopicInfo<String, Action> actions;
    private final TopicInfo<String, Transaction> transactions;
    private final Supplier<String> transactionIdSupplier;
    private final Supplier<String> entityIdSupplier;

    @Override
    public void build(KStreamBuilder builder) {
        builder
            .stream(actions.getKeySerde(), actions.getValueSerde(), actions.getName())
            .map((actionKey, action) -> KeyValue.pair(
                transactionIdSupplier.get(),
                new Transaction(action.deriveFacts(entityIdSupplier))
            ))
            .to(transactions.getKeySerde(), transactions.getValueSerde(), transactions.getName());
    }
}
