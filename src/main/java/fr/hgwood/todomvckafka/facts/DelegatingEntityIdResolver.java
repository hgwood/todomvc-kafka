package fr.hgwood.todomvckafka.facts;

import lombok.AllArgsConstructor;

import java.util.function.Function;

@AllArgsConstructor
public class DelegatingEntityIdResolver implements EntityIdResolver {
    private final Function<EntityLookup, EntityId> entityLookupResolver;
    private final Function<TemporaryId, EntityId> temporaryIdEntityIdResolver;
    private final Function<EntityId, EntityId> entityIdResolver;

    @Override
    public EntityId resolve(EntityLookup entityLookup) {
        return entityLookupResolver.apply(entityLookup);
    }

    @Override
    public EntityId resolve(TemporaryId temporaryId) {
        return temporaryIdEntityIdResolver.apply(temporaryId);
    }

    @Override
    public EntityId resolve(EntityId entityId) {
        return entityIdResolver.apply(entityId);
    }
}
