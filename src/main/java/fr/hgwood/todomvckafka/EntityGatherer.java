package fr.hgwood.todomvckafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import fr.hgwood.todomvckafka.support.json.JsonSerde;
import fr.hgwood.todomvckafka.support.kafkastreams.TopicInfo;
import fr.hgwood.todomvckafka.support.kafkastreams.Topology;
import io.vavr.collection.HashMap;
import io.vavr.collection.Map;
import io.vavr.collection.Set;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import static fr.hgwood.todomvckafka.Fact.FactKind.ASSERTION;

public class EntityGatherer implements Topology {

    private final TopicInfo<String, Transaction> transactions;
    private final TopicInfo<String, TodoItem> todoItems;
    private final Serde<Map> mapSerde;

    public EntityGatherer(
        TopicInfo<String, Transaction> transactions,
        TopicInfo<String, TodoItem> todoItems,
        ObjectMapper objectMapper
    ) {
        this.transactions = transactions;
        this.todoItems = todoItems;
        this.mapSerde = new JsonSerde<>(objectMapper, Map.class);
    }

    @Override
    public void build(KStreamBuilder builder) {
        builder
            .stream(transactions.getKeySerde(), transactions.getValueSerde(), transactions.getName())
            .mapValues(transaction -> this.mergeFacts(transaction.getFacts()))
            .flatMap((transactionKey, entities) -> entities.map(entity -> KeyValue.pair(entity._1, entity._2)))
            .to(todoItems.getKeySerde(), mapSerde, todoItems.getName());
    }

    private Map<String, Map> mergeFacts(Set<Fact> facts) {
        return facts
            .groupBy(Fact::getEntity)
            .mapValues(factsOfEntity -> factsOfEntity.foldLeft(HashMap.empty(), (fields, fact) -> {
                if (fact.getKind() == ASSERTION) {
                    return fields.put(fact.getAttribute().get().getName(), fact.getValue().get());
                } else {
                    return null;
                }
            }));
    }

}
