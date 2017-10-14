package fr.hgwood.todomvckafka;

import fr.hgwood.todomvckafka.actions.Action;
import fr.hgwood.todomvckafka.actions.DeriveFacts;
import fr.hgwood.todomvckafka.support.kafkastreams.RandomKey;
import fr.hgwood.todomvckafka.support.kafkastreams.TopicInfo;
import fr.hgwood.todomvckafka.support.kafkastreams.Topology;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.function.Supplier;

@RequiredArgsConstructor
@AllArgsConstructor
public class ActionsToFactRequests implements Topology {

    private final TopicInfo<String, Action> actions;
    private final TopicInfo<String, FactRequestTransaction> factRequests;
    private Supplier<String> transactionIdSupplier = RandomKey::randomKey;

    @Override
    public void build(KStreamBuilder builder) {
        builder
            .stream(actions.getKeySerde(), actions.getValueSerde(), actions.getName())
            .map((key, action) -> KeyValue.pair(
                transactionIdSupplier.get(),
                new FactRequestTransaction(new Transaction<>(action.accept(new DeriveFacts())))
            ))
            .to(factRequests.getKeySerde(), factRequests.getValueSerde(), factRequests.getName());
    }
}
