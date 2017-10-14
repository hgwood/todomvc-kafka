package fr.hgwood.todomvckafka.facts;

import io.vavr.control.Option;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.state.KeyValueStore;

@RequiredArgsConstructor
public class EntityExistenceBuilder2 implements FactRequestVisitor<Option<Fact>> {

    private final KeyValueStore<EntityId, EntityId> entityExistsStore;

    @Override
    public <R> Option<Fact> visit(AssertionRequest<R> factRequest) {
        EntityId entityId = new EntityId(factRequest.getTemporaryEntityId().getValue());
        entityExistsStore.put(entityId, entityId);
        Fact fact = new Assertion<>(entityId, factRequest.getAttribute(), factRequest.getValue());
        return Option.of(fact);
    }

    @Override
    public <R> Option<Fact> visit(AccretionRequest<R> factRequest) {
        EntityId entityId = new EntityId(factRequest.getEntityLookup().getValue());
        return Option
            .of(entityExistsStore.get(entityId))
            .map(entityId1 -> new Assertion<>(entityId1,
                factRequest.getAttribute(),
                factRequest.getValue()
            ));
    }

    @Override
    public Option<Fact> visit(EntityRetractionRequest factRequest) {
        EntityId entityId = new EntityId(factRequest.getEntityLookup().getValue());
        return Option.of(entityExistsStore.get(entityId)).map(EntityRetraction::new);
    }
}
