package fr.hgwood.todomvckafka2.reducers;

import fr.hgwood.todomvckafka2.actions.Action;
import fr.hgwood.todomvckafka2.actions.AddTodo;
import fr.hgwood.todomvckafka2.support.kafkastreams.TopicInfo;
import fr.hgwood.todomvckafka2.support.kafkastreams.Topology;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import static fr.hgwood.todomvckafka2.support.kafkastreams.RandomKey.randomKey;

@RequiredArgsConstructor
public class KnownEntitiesAdd implements Topology {

    private final TopicInfo<String, Action> actionsTopic;
    private final TopicInfo<String, String> createdEntities;

    @Override
    public void build(KStreamBuilder builder) {
        KStream<String, Action> actions = builder.stream(actionsTopic.getKeySerde(),
            actionsTopic.getValueSerde(),
            actionsTopic.getName()
        );

        actions
            .filter((key, action) -> action instanceof AddTodo)
            .map((key, action) -> {
                String entity = randomKey();
                return KeyValue.pair(entity, entity);
            })
            .to(createdEntities.getKeySerde(),
                createdEntities.getValueSerde(),
                createdEntities.getName()
            );
    }
}
