package fr.hgwood.todomvckafka.facts;

import fr.hgwood.todomvckafka.support.kafkastreams.RandomKey;
import io.vavr.Function1;
import io.vavr.control.Option;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.function.Supplier;

@RequiredArgsConstructor
public class EntityExistenceBuilder2 implements FactRequestVisitor<Option<Fact>> {

    private final KeyValueStore<EntityId, EntityId> entityExistsStore;

    @Override
    public Option<Fact> visit(AssertionRequest factRequest) {
        EntityId entityId = new EntityId(factRequest.getTemporaryEntityId().getValue());
        entityExistsStore.put(entityId, entityId);
        Fact fact = new Assertion<>(entityId, factRequest.getAttribute(), factRequest.getValue());
        return Option.of(fact);
    }

    @Override
    public Option<Fact> visit(AccretionRequest factRequest) {
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
        return Option
            .of(entityExistsStore.get(entityId))
            .map(entityId1 -> new EntityRetraction(entityId1));
    }
}
