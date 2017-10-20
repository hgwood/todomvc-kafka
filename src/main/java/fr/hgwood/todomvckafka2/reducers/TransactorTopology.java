package fr.hgwood.todomvckafka2.reducers;

import fr.hgwood.todomvckafka2.actions.Action;
import fr.hgwood.todomvckafka2.actions.AddTodo;
import fr.hgwood.todomvckafka2.actions.RejectedAction;
import fr.hgwood.todomvckafka2.facts.Transaction;
import fr.hgwood.todomvckafka2.support.kafkastreams.TopicInfo;
import fr.hgwood.todomvckafka2.support.kafkastreams.Topology;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import static fr.hgwood.todomvckafka2.support.kafkastreams.RandomKey.randomKey;

@RequiredArgsConstructor
public class TransactorTopology implements Topology {

    private static final String TRANSACTOR_PROCESSOR_NAME = "transactor-processor";
    private static final String ACTIONS_SOURCE_NAME = "actions-source";
    private static final String TRANSACTIONS_SINK_NAME = "transactions-sink";
    private static final String REJECTED_ACTIONS_SINK_NAME = "rejected-actions-sink";


    private final TopicInfo<String, Action> actions;
    private final TopicInfo<String, Transaction> transactions;
    private final TopicInfo<String, RejectedAction> rejectedActions;
    private final TopicInfo<String, String> knownEntities;

    @Override
    public void build(KStreamBuilder builder) {
        KTable<String, String> knownEntitiesTable = builder.table(knownEntities.getKeySerde(),
            knownEntities.getValueSerde(),
            knownEntities.getName()
        );
        
        KStream<String, Action> a = builder
            .stream(actions.getKeySerde(), actions.getValueSerde(), actions.getName());
        a
            .filter((key, action) -> action instanceof AddTodo)
            .map((key, action) -> {
                String entity = randomKey();
                return KeyValue.pair(entity, entity);
            })
            .to(knownEntities.getKeySerde(),
                knownEntities.getValueSerde(),
                knownEntities.getName()
            );
        a
            .process(
                () -> new Transactor(TRANSACTIONS_SINK_NAME,
                    REJECTED_ACTIONS_SINK_NAME,
                    knownEntities.getName()
                ),
                knownEntitiesTable.queryableStoreName()
            );


        /*StateStoreSupplier knownEntitiesStore = Stores
            .create(knownEntities.getName())
            .withKeys(knownEntities.getKeySerde())
            .withValues(knownEntities.getValueSerde())
            .persistent()
            .build();*/

        /*builder.addSource(ACTIONS_SOURCE_NAME,
            actions.getKeySerde().deserializer(),
            actions.getValueSerde().deserializer(),
            actions.getName()
        );*/

        /*builder.addProcessor(TRANSACTOR_PROCESSOR_NAME,
            () -> new Transactor(TRANSACTIONS_SINK_NAME,
                REJECTED_ACTIONS_SINK_NAME,
                knownEntities.getName()
            ),
            ACTIONS_SOURCE_NAME
        );*/

        //builder.addStateStore(knownEntitiesStore, TRANSACTOR_PROCESSOR_NAME);
        /*builder.connectProcessorAndStateStores(TRANSACTOR_PROCESSOR_NAME,
            knownEntitiesTable.queryableStoreName()
        );*/

        builder.addSink(TRANSACTIONS_SINK_NAME,
            transactions.getName(),
            transactions.getKeySerde().serializer(),
            transactions.getValueSerde().serializer(),
            TRANSACTOR_PROCESSOR_NAME
        );

        builder.addSink(REJECTED_ACTIONS_SINK_NAME,
            rejectedActions.getName(),
            rejectedActions.getKeySerde().serializer(),
            rejectedActions.getValueSerde().serializer(),
            TRANSACTOR_PROCESSOR_NAME
        );
    }
}
