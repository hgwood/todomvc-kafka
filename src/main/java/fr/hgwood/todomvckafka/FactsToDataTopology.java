package fr.hgwood.todomvckafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import fr.hgwood.todomvckafka.support.json.JsonSerde;
import fr.hgwood.todomvckafka.support.kafkastreams.TopicInfo;
import fr.hgwood.todomvckafka.support.kafkastreams.Topology;
import io.vavr.collection.HashMap;
import lombok.AllArgsConstructor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStreamBuilder;

@AllArgsConstructor
public class FactsToDataTopology implements Topology {

    private final TopicInfo<String, Fact> facts;
    private final TopicInfo<String, TodoItem> todoItems;
    private final ObjectMapper objectMapper;

    @Override
    public void build(KStreamBuilder builder) {
        builder
            .stream(facts.getKeySerde(), facts.getValueSerde(), facts.getName())
            .groupBy((factKey, fact) -> fact.getEntity(),
                Serdes.String(),
                facts.getValueSerde()
            )
            .aggregate(() -> HashMap.<String, Object>empty(),
                (entityKey, fact, entity) -> entity.put(fact
                    .getAttribute()
                    .get()
                    .getName(), fact.getValue().get()),
                new JsonSerde<>(objectMapper, HashMap.class),
                "todo-items-aggregation-store"
            )
            .mapValues(value -> objectMapper.convertValue(value,
                TodoItem.class
            ))
            .to(todoItems.getKeySerde(),
                todoItems.getValueSerde(),
                todoItems.getName()
            );
    }
}
