package fr.hgwood.todomvckafka.facts;

import io.vavr.collection.HashSet;
import io.vavr.collection.Set;

public class CollectEntityLookups implements FactRequestVisitor<Set<EntityId>> {

    @Override
    public Set<EntityId> visit(AssertionRequest factRequest) {
        return HashSet.empty();
    }

    @Override
    public Set<EntityId> visit(AccretionRequest factRequest) {
        return HashSet.of(new EntityId(factRequest.getEntityLookup().getValue()));
    }

    @Override
    public Set<EntityId> visit(EntityRetractionRequest factRequest) {
        return HashSet.of(new EntityId(factRequest.getEntityLookup().getValue()));
    }
}
