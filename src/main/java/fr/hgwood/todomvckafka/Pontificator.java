package fr.hgwood.todomvckafka;

import fr.hgwood.todomvckafka.actions.Action;
import fr.hgwood.todomvckafka.support.kafkastreams.TopicInfo;
import fr.hgwood.todomvckafka.support.kafkastreams.Topology;
import lombok.AllArgsConstructor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.Stores;

import java.util.function.Supplier;

@AllArgsConstructor
public class Pontificator implements Topology {

    private static final String ACTION_PROCESSOR = "action-processor";
    private static final String ACTION_SOURCE = "action-source";
    private static final String TRANSACTIONS_SINK = "transactions-sink";
    private static final String IGNORED_ACTIONS_SINK = "ignored-actions-sink";

    private final TopicInfo<String, Action> actions;
    private final TopicInfo<String, Action> ignoredActions;
    private final TopicInfo<String, Transaction> transactions;
    private final Supplier<String> transactionIdSupplier;
    private final Supplier<String> entityIdSupplier;
    private final TopicInfo<String, Boolean> entityExistsStoreInfo;

    @Override
    public void build(KStreamBuilder builder) {
        StateStoreSupplier entityExistsStore = Stores
            .create(entityExistsStoreInfo.getName())
            .withKeys(entityExistsStoreInfo.getKeySerde())
            .withValues(entityExistsStoreInfo.getValueSerde())
            .persistent()
            .build();

        builder.addSource(ACTION_SOURCE,
            actions.getKeySerde().deserializer(),
            actions.getValueSerde().deserializer(),
            actions.getName()
        );

        builder.addProcessor(ACTION_PROCESSOR,
            () -> new ActionProcessor(entityExistsStoreInfo.getName(),
                transactionIdSupplier,
                entityIdSupplier
            ),
            ACTION_SOURCE
        );

        builder.addStateStore(entityExistsStore, ACTION_PROCESSOR);

        builder.addSink(TRANSACTIONS_SINK,
            transactions.getName(),
            transactions.getKeySerde().serializer(),
            transactions.getValueSerde().serializer(),
            ACTION_PROCESSOR
        );

        builder.addSink(IGNORED_ACTIONS_SINK,
            ignoredActions.getName(),
            ignoredActions.getKeySerde().serializer(),
            ignoredActions.getValueSerde().serializer(),
            ACTION_PROCESSOR
        );
    }
}
