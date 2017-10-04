package fr.hgwood.todomvckafka.facts;

import lombok.AllArgsConstructor;

import java.util.function.Function;

@AllArgsConstructor
public class DelegatingEntityIdResolver implements EntityIdResolver {
    private final Function<EntityLookup, EntityId> entityLookupResolver;
    private final Function<TemporaryEntityId, EntityId> temporaryIdEntityIdResolver;

    @Override
    public EntityId resolve(EntityLookup entityLookup) {
        return entityLookupResolver.apply(entityLookup);
    }

    @Override
    public EntityId resolve(TemporaryEntityId temporaryEntityId) {
        return temporaryIdEntityIdResolver.apply(temporaryEntityId);
    }
}
