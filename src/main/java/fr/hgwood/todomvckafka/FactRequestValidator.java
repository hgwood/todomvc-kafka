package fr.hgwood.todomvckafka;

import fr.hgwood.todomvckafka.facts.CollectEntityLookups;
import fr.hgwood.todomvckafka.facts.EntityId;
import fr.hgwood.todomvckafka.facts.FactRequest;
import fr.hgwood.todomvckafka.facts.FactRequestVisitor;
import fr.hgwood.todomvckafka.support.kafkastreams.TopicInfo;
import fr.hgwood.todomvckafka.support.kafkastreams.Topology;
import io.vavr.collection.Set;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

@RequiredArgsConstructor
public class FactRequestValidator implements Topology {

    private final TopicInfo<String, Transaction<FactRequest>> factRequests;
    private final TopicInfo<EntityId, EntityId> entityIds;

    @Override
    public void build(KStreamBuilder builder) {
        KTable<EntityId, EntityId> entities = builder.table(entityIds.getKeySerde(), entityIds.getValueSerde(), entityIds.getName());

        /*builder.stream(factRequests.getKeySerde(), factRequests.getValueSerde(), factRequests.getName())
            .map((key, transaction) -> {
                FactRequestVisitor<Set<EntityId>> visitor = new CollectEntityLookups();
                Set<EntityId> entityIds = transaction.getFacts().flatMap(factRequest -> factRequest.accept(visitor));

            })*/
    }
}
