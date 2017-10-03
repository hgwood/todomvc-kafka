package fr.hgwood.todomvckafka.facts;

import fr.hgwood.todomvckafka.schema.Attribute;
import lombok.Value;

import java.util.UUID;

@Value
public class EntityLookup implements EntityRef {
    private final Attribute<UUID> attribute;
    private final UUID value;

    @Override
    public EntityId resolve(EntityIdResolver resolver) {
        return resolver.resolve(this);
    }
}
