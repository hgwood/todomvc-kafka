package fr.hgwood.todomvckafka2.reducers;

import fr.hgwood.todomvckafka2.actions.Action;
import fr.hgwood.todomvckafka2.actions.DeleteTodo;
import fr.hgwood.todomvckafka2.support.kafkastreams.TopicInfo;
import fr.hgwood.todomvckafka2.support.kafkastreams.Topology;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

@RequiredArgsConstructor
public class KnownEntitiesDelete implements Topology {

    private final TopicInfo<String, Action> actionsTopic;
    private final TopicInfo<String, String> createdEntitiesTopic;
    private final TopicInfo<String, DeleteTodo> validDeletesTopic;
    private final TopicInfo<String, DeleteTodo> rejectedDeletesTopic;


    @Override
    public void build(KStreamBuilder builder) {
        KStream<String, Action> actions = builder.stream(actionsTopic.getKeySerde(),
            actionsTopic.getValueSerde(),
            actionsTopic.getName()
        );

        KTable<String, String> createdEntities = builder.table(createdEntitiesTopic.getKeySerde(),
            createdEntitiesTopic.getValueSerde(),
            createdEntitiesTopic.getName()
        );

        KStream<String, DeleteTodo> deletes = actions.filter((key, action) -> action instanceof DeleteTodo).mapValues(action -> (DeleteTodo)action);


        KStream<String, DeleteTodo>[] branches = deletes
            .selectKey((key, action) -> action.getId())
            .leftJoin(createdEntities, (action, entity) -> {
                if (entity != null) {
                    return action;
                } else {
                    return null;
                }
            })
            .branch((key, action) -> action != null, (key, action) -> true);
        KStream<String, DeleteTodo> validDeletes = branches[0];
        KStream<String, DeleteTodo> rejectedDeletes = branches[1];

        validDeletes.to(validDeletesTopic.getKeySerde(), validDeletesTopic.getValueSerde(), validDeletesTopic.getName());

        //rejectedDeletes.
    }
}
