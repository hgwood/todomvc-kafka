package fr.hgwood.todomvckafka.facts;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.vavr.collection.Map;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY)
@JsonSubTypes({
    @JsonSubTypes.Type(name = "value-assertion", value = ValueAssertion.class),
    @JsonSubTypes.Type(name = "entity-retraction", value = EntityRetraction.class)
})
public interface Fact {
    String getEntity();

    Map<String, Object> apply(Map<String, Object> entity);
}
