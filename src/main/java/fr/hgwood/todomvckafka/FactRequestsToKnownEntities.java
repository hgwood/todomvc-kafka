package fr.hgwood.todomvckafka;

import fr.hgwood.todomvckafka.facts.EntityExistenceBuilder;
import fr.hgwood.todomvckafka.facts.EntityId;
import fr.hgwood.todomvckafka.facts.FactRequest;
import fr.hgwood.todomvckafka.facts.FactRequestVisitor;
import fr.hgwood.todomvckafka.support.kafkastreams.TopicInfo;
import fr.hgwood.todomvckafka.support.kafkastreams.Topology;
import io.vavr.collection.Set;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.HashMap;

@RequiredArgsConstructor
public class FactRequestsToKnownEntities implements Topology {

    private final TopicInfo<String, Transaction<FactRequest>> factRequests;
    private final TopicInfo<EntityId, EntityId> entityIds;

    @Override
    public void build(KStreamBuilder builder) {
        builder.stream(factRequests.getKeySerde(), factRequests.getValueSerde(), factRequests.getName())
            .flatMap((key, transaction) -> {
                FactRequestVisitor<Set<EntityId>> visitor = new EntityExistenceBuilder(new HashMap<>());
                Set<EntityId> entities = transaction.getFacts().flatMap(factRequest -> factRequest.accept(visitor));
                return entities.map(entity -> KeyValue.pair(entity, entity));
            })
            .to(entityIds.getKeySerde(), entityIds.getValueSerde(), entityIds.getName());
    }
}
