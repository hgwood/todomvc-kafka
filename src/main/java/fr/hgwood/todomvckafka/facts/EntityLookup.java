package fr.hgwood.todomvckafka.facts;

import fr.hgwood.todomvckafka.schema.Attribute;
import lombok.Value;

import java.util.UUID;

@Value
public class EntityLookup {
    private final Attribute<String> attribute;
    private final String value;
}
