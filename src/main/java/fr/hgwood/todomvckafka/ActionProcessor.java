package fr.hgwood.todomvckafka;

import fr.hgwood.todomvckafka.actions.Action;
import fr.hgwood.todomvckafka.actions.todoitem.AddTodo;
import fr.hgwood.todomvckafka.actions.todoitem.DeleteTodo;
import fr.hgwood.todomvckafka.facts.Fact;
import io.vavr.collection.Set;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.function.Supplier;

@RequiredArgsConstructor
public class ActionProcessor extends AbstractProcessor<String, Action> {

    private final String entityExistsStoreName;
    private final Supplier<String> transactionIdSupplier;
    private final Supplier<String> entityIdSupplier;
    private KeyValueStore<String, Boolean> entityExistsStore;

    @Override
    public void init(ProcessorContext context) {
        super.init(context);
        this.entityExistsStore =
            (KeyValueStore<String, Boolean>) context.getStateStore(entityExistsStoreName);
    }

    @Override
    public void process(String key, Action action) {
        Set<Fact> facts = action.deriveFacts(entityIdSupplier);
        if (action instanceof AddTodo) {
            entityExistsStore.put(facts.toList().get(0).getEntity(), true);
        } else if (action instanceof DeleteTodo) {
            Boolean entityExists = entityExistsStore.get(((DeleteTodo) action).getId());
            if (entityExists == Boolean.TRUE) {
                entityExistsStore.put(((DeleteTodo) action).getId(), false);
            } else {
                this.context().forward(key, action, 1);
                return;
            }
        }
        this.context().forward(transactionIdSupplier.get(), new Transaction(facts), 0);
        this.context().commit();
    }

}
