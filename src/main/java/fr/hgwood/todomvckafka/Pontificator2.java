package fr.hgwood.todomvckafka;

import fr.hgwood.todomvckafka.facts.EntityId;
import fr.hgwood.todomvckafka.facts.Fact;
import fr.hgwood.todomvckafka.facts.FactRequest;
import fr.hgwood.todomvckafka.support.kafkastreams.TopicInfo;
import fr.hgwood.todomvckafka.support.kafkastreams.Topology;
import lombok.AllArgsConstructor;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.Stores;

@AllArgsConstructor
public class Pontificator2 implements Topology {

    private static final String FACT_REQUEST_PROCESSOR = "action-processor";
    private static final String FACT_REQUEST_SOURCE = "action-source";
    private static final String ACCEPTED_TRANSACTION_SINK = "acceptedTransactions-sink";
    private static final String REJECTED_TRANSACTION_SINK = "ignored-actions-sink";

    private final TopicInfo<String, FactRequestTransaction> actions;
    private final TopicInfo<String, FactRequestTransaction> rejectedTransactions;
    private final TopicInfo<String, FactTransaction> acceptedTransactions;
    //private final Supplier<String> transactionIdSupplier;
    //private final Supplier<String> entityIdSupplier;
    private final TopicInfo<EntityId, EntityId> entityExistsStoreInfo;

    @Override
    public void build(KStreamBuilder builder) {
        StateStoreSupplier entityExistsStore = Stores
            .create(entityExistsStoreInfo.getName())
            .withKeys(entityExistsStoreInfo.getKeySerde())
            .withValues(entityExistsStoreInfo.getValueSerde())
            .persistent()
            .build();

        builder.addSource(FACT_REQUEST_SOURCE,
            actions.getKeySerde().deserializer(),
            actions.getValueSerde().deserializer(),
            actions.getName()
        );

        builder.addProcessor(FACT_REQUEST_PROCESSOR,
            () -> new FactRequestsProcessor(entityExistsStoreInfo.getName()),
            FACT_REQUEST_SOURCE
        );

        builder.addStateStore(entityExistsStore, FACT_REQUEST_PROCESSOR);

        builder.addSink(ACCEPTED_TRANSACTION_SINK,
            acceptedTransactions.getName(),
            acceptedTransactions.getKeySerde().serializer(),
            acceptedTransactions.getValueSerde().serializer(),
            FACT_REQUEST_PROCESSOR
        );

        builder.addSink(REJECTED_TRANSACTION_SINK,
            rejectedTransactions.getName(),
            rejectedTransactions.getKeySerde().serializer(),
            rejectedTransactions.getValueSerde().serializer(),
            FACT_REQUEST_PROCESSOR
        );
    }
}
