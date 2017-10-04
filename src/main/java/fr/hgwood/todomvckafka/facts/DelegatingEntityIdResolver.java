package fr.hgwood.todomvckafka.facts;

import io.vavr.control.Option;
import io.vavr.control.Try;
import lombok.AllArgsConstructor;

import java.util.function.Function;

@AllArgsConstructor
public class DelegatingEntityIdResolver implements EntityIdResolver {
    private final Function<EntityLookup, Try<Option<EntityId>>> entityLookupResolver;
    private final Function<TemporaryEntityId, EntityId> temporaryIdEntityIdResolver;

    @Override
    public Try<Option<EntityId>> resolve(EntityLookup entityLookup) {
        return entityLookupResolver.apply(entityLookup);
    }

    @Override
    public EntityId resolve(TemporaryEntityId temporaryEntityId) {
        return temporaryIdEntityIdResolver.apply(temporaryEntityId);
    }
}
