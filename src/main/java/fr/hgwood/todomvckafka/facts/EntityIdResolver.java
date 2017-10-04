package fr.hgwood.todomvckafka.facts;

import io.vavr.control.Either;

public interface EntityIdResolver {
    Either<NoSuchEntity, EntityId> resolve(EntityLookup entityLookup);

    EntityId resolve(TemporaryEntityId temporaryEntityId);
}
