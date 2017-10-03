package fr.hgwood.todomvckafka.facts;

public interface EntityRef {
    EntityId resolve(EntityIdResolver resolver);

    class EntityId implements EntityRef {

        @Override
        public EntityId resolve(EntityIdResolver resolver) {
            return null;
        }
    }
}
