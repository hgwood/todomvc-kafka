package fr.hgwood.todomvckafka.facts;

import io.vavr.control.Either;
import lombok.AllArgsConstructor;

import java.util.function.Function;

@AllArgsConstructor
public class DelegatingEntityIdResolver implements EntityIdResolver {
    private final Function<EntityLookup, Either<NoSuchEntity, EntityId>> entityLookupResolver;
    private final Function<TemporaryEntityId, EntityId> temporaryIdEntityIdResolver;

    @Override
    public Either<NoSuchEntity, EntityId> resolve(EntityLookup entityLookup) {
        return entityLookupResolver.apply(entityLookup);
    }

    @Override
    public EntityId resolve(TemporaryEntityId temporaryEntityId) {
        return temporaryIdEntityIdResolver.apply(temporaryEntityId);
    }
}
