package fr.hgwood.todomvckafka.facts;

public class EntityId implements EntityRef {

    @Override
    public EntityId resolve(EntityIdResolver resolver) {
        return resolver.resolve(this);
    }
}
