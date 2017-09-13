package fr.hgwood.todomvckafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import fr.hgwood.todomvckafka.support.json.JsonSerde;
import fr.hgwood.todomvckafka.support.kafkastreams.TopicInfo;
import fr.hgwood.todomvckafka.support.kafkastreams.Topology;
import io.vavr.collection.HashMap;
import lombok.Value;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import static fr.hgwood.todomvckafka.Fact.FactKind.ASSERTION;

public class EntityGatherer implements Topology {

    private final TopicInfo<String, Fact> facts;
    private final TopicInfo<String, TodoItem> todoItems;
    private final ObjectMapper objectMapper;
    private final Serde<GroupKey> groupKeySerde;
    private final Serde<HashMap> mapSerde;

    public EntityGatherer(
        TopicInfo<String, Fact> facts,
        TopicInfo<String, TodoItem> todoItems,
        ObjectMapper objectMapper
    ) {
        this.facts = facts;
        this.todoItems = todoItems;
        this.objectMapper = objectMapper;
        this.groupKeySerde = new JsonSerde<>(objectMapper, GroupKey.class);
        this.mapSerde = new JsonSerde<>(objectMapper, HashMap.class);
    }

    @Override
    public void build(KStreamBuilder builder) {
        KStream<GroupKey, HashMap> stream = builder
            .stream(facts.getKeySerde(), facts.getValueSerde(), facts.getName())
            .groupBy(this::groupId, groupKeySerde, facts.getValueSerde())
            .aggregate(HashMap::empty, this::reduceFields, mapSerde)
            .toStream();
        stream.print(groupKeySerde, mapSerde);
        stream
            .groupBy((groupKey, fields) -> groupKey.getEntity(), Serdes.String(), mapSerde)
            .aggregate(HashMap::empty, this::mergeMaps, mapSerde)
            .to(todoItems.getKeySerde(), mapSerde, todoItems.getName());
    }

    private GroupKey groupId(String key, Fact fact) {
        return new GroupKey(fact.getTransaction(), fact.getEntity());
    }

    private HashMap<String, Object> reduceFields(
        GroupKey groupKey, Fact fact, HashMap<String, Object> fields
    ) {
        // move this to Fact#toMap
        if (fact.getKind() == ASSERTION) {
            return fields.put(fact.getAttribute().get().getName(), fact.getValue().get());
        } else {
            return null;
        }
    }

    private HashMap<String, Object> mergeMaps(
        String entity, HashMap<String, Object> newFields, HashMap<String, Object> fields
    ) {
        return fields.merge(newFields);
    }

    @Value
    private static class GroupKey {
        private final Transaction.Id transaction;
        private final String entity;
    }
}
