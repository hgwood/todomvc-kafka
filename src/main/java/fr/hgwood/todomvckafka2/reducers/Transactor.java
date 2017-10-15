package fr.hgwood.todomvckafka2.reducers;

import fr.hgwood.todomvckafka2.actions.*;
import fr.hgwood.todomvckafka2.facts.Assertion;
import fr.hgwood.todomvckafka2.facts.EntityRetraction;
import fr.hgwood.todomvckafka2.facts.Transaction;
import fr.hgwood.todomvckafka2.schema.Attribute;
import io.vavr.collection.HashSet;
import io.vavr.collection.Set;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import static fr.hgwood.todomvckafka2.support.kafkastreams.RandomKey.randomKey;
import static java.lang.String.format;

@RequiredArgsConstructor
public class Transactor extends AbstractProcessor<String, Action> implements ActionProcessor<String> {

    private final String transactionsChildName;
    private final String rejectedActionsChildName;
    private final String knownEntitiesStoreName;
    private KeyValueStore<String, String> knownEntities;

    @Override
    public void init(ProcessorContext context) {
        super.init(context);
        this.knownEntities =
            (KeyValueStore<String, String>) context.getStateStore(knownEntitiesStoreName);
    }

    @Override
    public void process(String key, AddTodo action) {
        String entity = randomKey();
        this
            .context()
            .forward(randomKey(),
                new Transaction(HashSet.of(new Assertion<>(entity,
                        Attribute.TODO_ITEM_TEXT,
                        action.getText()
                    ),
                    new Assertion<>(entity, Attribute.TODO_ITEM_COMPLETED, false)
                )),
                transactionsChildName
            );
        this.knownEntities.put(entity, entity);
    }

    @Override
    public void process(String key, EditTodo action) {
        if (this.knownEntities.get(action.getId()) != null) {
            this.context().forward(randomKey(),
                new Transaction(HashSet.of(new Assertion<>(action.getId(),
                    Attribute.TODO_ITEM_TEXT,
                    action.getText()
                ))),
                transactionsChildName
            );
        } else {
            this.context().forward(key,
                new RejectedAction(action, RejectionMessages.unknownEntity(action.getId())),
                rejectedActionsChildName
            );
        }
    }

    @Override
    public void process(String key, DeleteTodo action) {
        if (this.knownEntities.get(action.getId()) != null) {
            this.context().forward(randomKey(),
                new Transaction(HashSet.of(new EntityRetraction(action.getId()))),
                transactionsChildName
            );
        } else {
            this.context().forward(key,
                new RejectedAction(action, RejectionMessages.unknownEntity(action.getId())),
                rejectedActionsChildName
            );
        }
    }

    @Override
    public void process(String key, CompleteAll action) {
        Set<String> knownEntitiesSnapshot = HashSet.empty();
        this.knownEntities.all().forEachRemaining(kv -> {
            knownEntitiesSnapshot.add(kv.key);
        });
        this
            .context()
            .forward(
                randomKey(),
                new Transaction(knownEntitiesSnapshot.map(entity -> new Assertion<>(entity,
                    Attribute.TODO_ITEM_COMPLETED,
                    true
                )))
            );
    }

    @Override
    public void process(String key, Action action) {
        action.accept(key, this);
    }

    public static class RejectionMessages {
        private static final String UNKNOWN_ENTITY = "entity '%s' is unknown";

        public static String unknownEntity(String entity) {
            return format(UNKNOWN_ENTITY, entity);
        }
    }
}
