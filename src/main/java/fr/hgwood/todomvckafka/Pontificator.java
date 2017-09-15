package fr.hgwood.todomvckafka;

import fr.hgwood.todomvckafka.support.kafkastreams.TopicInfo;
import fr.hgwood.todomvckafka.support.kafkastreams.Topology;
import io.vavr.collection.HashSet;
import lombok.AllArgsConstructor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.function.Supplier;

import static fr.hgwood.todomvckafka.schema.Attribute.TODO_ITEM_COMPLETED;
import static fr.hgwood.todomvckafka.schema.Attribute.TODO_ITEM_TEXT;

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
            .map((actionKey, action) -> {
                if (action.getType() == ActionType.ADD_TODO) {
                    String entity = entityIdSupplier.get();
                    return KeyValue.pair(transactionIdSupplier.get(), new Transaction(HashSet.of(
                        Fact.of(entity, TODO_ITEM_TEXT, action.getText()),
                        Fact.of(entity, TODO_ITEM_COMPLETED, false)
                    )));
                } else if (action.getType() == ActionType.DELETE_TODO) {
                    return KeyValue.pair(
                        transactionIdSupplier.get(),
                        new Transaction(HashSet.of(Fact.retractEntity(action.getId())))
                    );
                } else {
                    return null;
                }
            })
            .to(transactions.getKeySerde(), transactions.getValueSerde(), transactions.getName());
    }
}
