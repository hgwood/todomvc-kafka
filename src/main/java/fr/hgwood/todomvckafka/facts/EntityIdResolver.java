package fr.hgwood.todomvckafka.facts;

public interface EntityIdResolver {
    EntityId resolve(EntityLookup entityLookup);
    EntityId resolve(TemporaryId temporaryId);
    EntityId resolve(EntityId entityId);
}
