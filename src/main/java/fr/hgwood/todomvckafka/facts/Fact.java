package fr.hgwood.todomvckafka.facts;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import fr.hgwood.todomvckafka.schema.Attribute;
import io.vavr.collection.Map;

import java.util.function.Function;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY)
@JsonSubTypes({
    @JsonSubTypes.Type(name = "value-assertion", value = ValueAssertion.class),
    @JsonSubTypes.Type(name = "entity-retraction", value = EntityRetraction.class)
})
public interface Fact {
    EntityRef getEntity();

    Map<String, Object> apply(Map<String, Object> entity);

    static <T> Fact assertion(String temporaryId, Attribute<T> attribute, T value) {
        return new ValueAssertion<>(new TemporaryId(temporaryId), attribute, value);
    }

    default Fact resolveEntity(Function<EntityLookup, EntityId> resolveLookup, Function<TemporaryId, EntityId> resolveTemporaryId) {
        EntityRef entity = this.getEntity();
        if (entity instanceof EntityLookup) {
            return resolveLookup.apply((EntityLookup)entity);
        }
    }
}
