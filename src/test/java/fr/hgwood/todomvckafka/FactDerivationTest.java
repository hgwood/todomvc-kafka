package fr.hgwood.todomvckafka;

import fr.hgwood.todomvckafka.actions.todoitem.AddTodo;
import fr.hgwood.todomvckafka.facts.Fact;
import fr.hgwood.todomvckafka.facts.ValueAssertion;
import io.vavr.collection.HashSet;
import io.vavr.collection.Set;
import org.junit.Test;

import static fr.hgwood.todomvckafka.schema.Attribute.TODO_ITEM_COMPLETED;
import static fr.hgwood.todomvckafka.schema.Attribute.TODO_ITEM_TEXT;
import static org.junit.Assert.assertEquals;

public class FactDerivationTest {

    @Test
    public void test() {
        String expectedEntityId = "test-entity-id";
        String expectedText = "test-todo-item-text";
        Set<Fact> expected =
            HashSet.of(new ValueAssertion<>(expectedEntityId, TODO_ITEM_TEXT, expectedText),
                new ValueAssertion<>(expectedEntityId, TODO_ITEM_COMPLETED, false)
            );

        Set<Fact> actual = new AddTodo(expectedText).deriveFacts(() -> expectedEntityId);

        assertEquals(expected, actual);
    }

}
