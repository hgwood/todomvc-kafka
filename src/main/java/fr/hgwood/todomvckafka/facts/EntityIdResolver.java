package fr.hgwood.todomvckafka.facts;

import io.vavr.control.Option;
import io.vavr.control.Try;

public interface EntityIdResolver {
    Try<Option<EntityId>> resolve(EntityLookup entityLookup);
    EntityId resolve(TemporaryEntityId temporaryEntityId);
}
