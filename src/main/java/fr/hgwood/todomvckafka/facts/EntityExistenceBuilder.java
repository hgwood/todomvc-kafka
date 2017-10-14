package fr.hgwood.todomvckafka.facts;

import io.vavr.collection.HashSet;
import io.vavr.collection.Set;
import lombok.RequiredArgsConstructor;

import java.util.Map;

import static fr.hgwood.todomvckafka.support.kafkastreams.RandomKey.randomKey;

@RequiredArgsConstructor
public class EntityExistenceBuilder implements FactRequestVisitor<Set<EntityId>> {

    private final Map<TemporaryEntityId, EntityId> ids;

    @Override
    public Set<EntityId> visit(AssertionRequest factRequest) {
        EntityId entityId = ids.putIfAbsent(factRequest.getTemporaryEntityId(), new EntityId(randomKey()));
        return HashSet.of(entityId);
    }

    @Override
    public Set<EntityId> visit(AccretionRequest factRequest) {
        return HashSet.empty();
    }

    @Override
    public Set<EntityId> visit(EntityRetractionRequest factRequest) {
        return HashSet.empty();
    }
}
