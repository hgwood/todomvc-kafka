package fr.hgwood.todomvckafka2.reducers;

import fr.hgwood.todomvckafka2.actions.*;
import fr.hgwood.todomvckafka2.facts.Assertion;
import fr.hgwood.todomvckafka2.facts.Transaction;
import fr.hgwood.todomvckafka2.schema.Attribute;
import fr.hgwood.todomvckafka2.support.kafkastreams.TopicInfo;
import fr.hgwood.todomvckafka2.support.kafkastreams.Topology;
import io.vavr.collection.HashSet;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.processor.AbstractProcessor;

import static fr.hgwood.todomvckafka2.support.kafkastreams.RandomKey.randomKey;

@RequiredArgsConstructor
public class CreatedEntities implements Topology {
    private static final String ADD_TODO_PROCESSOR_NAME = "add-todo-processor";
    private static final String DELETE_TODO_ID_CHECK_PROCESSOR_NAME = "delete-todo-id-check-processor";

    private static final String ACTIONS_SOURCE_NAME = "actions-source";

    private static final String ACTIONS_SINK_NAME = "actions-sink";
    private static final String CREATED_ENTITIES_SINK_NAME = "created-entities-sink";
    private static final String TRANSACTIONS_SINK_NAME = "transactions-sink";

    private final TopicInfo<String, Action> inputActions;
    private final TopicInfo<String, Action> outputActions;
    private final TopicInfo<String, Transaction> transactions;
    private final TopicInfo<String, String> createdEntities;

    @Override
    public void build(KStreamBuilder builder) {
        builder.addSource(ACTIONS_SOURCE_NAME,
            inputActions.getKeySerde().deserializer(),
            inputActions.getValueSerde().deserializer(),
            inputActions.getName()
        );

        builder.addProcessor(ADD_TODO_PROCESSOR_NAME, ProcAdd::new, ACTIONS_SOURCE_NAME);

        builder.addSink(ACTIONS_SINK_NAME,
            outputActions.getName(),
            outputActions.getKeySerde().serializer(),
            outputActions.getValueSerde().serializer(), ADD_TODO_PROCESSOR_NAME
        );

        builder.addSink(TRANSACTIONS_SINK_NAME,
            transactions.getName(),
            transactions.getKeySerde().serializer(),
            transactions.getValueSerde().serializer(), ADD_TODO_PROCESSOR_NAME
        );

        builder.addSink(CREATED_ENTITIES_SINK_NAME,
            createdEntities.getName(),
            createdEntities.getKeySerde().serializer(),
            createdEntities.getValueSerde().serializer(), ADD_TODO_PROCESSOR_NAME
        );
    }

    private static class ProcAdd extends AbstractProcessor<String, Action> implements ActionProcessor<String> {

        @Override
        public void process(String key, AddTodo action) {
            String entity = randomKey();
            Transaction transaction = new Transaction(HashSet.of(new Assertion<>(entity,
                    Attribute.TODO_ITEM_TEXT,
                    action.getText()
                ),
                new Assertion<>(entity, Attribute.TODO_ITEM_COMPLETED, false)
            ));
            this.context().forward(key, action, ACTIONS_SINK_NAME);
            this.context().forward(randomKey(), transaction, TRANSACTIONS_SINK_NAME);
            this.context().forward(entity, key, CREATED_ENTITIES_SINK_NAME);
        }

        @Override
        public void process(String key, EditTodo action) {

        }

        @Override
        public void process(String key, DeleteTodo action) {

        }

        @Override
        public void process(String key, CompleteAll action) {

        }

        @Override
        public void process(String key, ClearCompleted action) {

        }

        @Override
        public void process(String key, Action action) {
            action.accept(key, this);
        }
    }

    private static class DeleteTodoIdCheckProcessor extends AbstractProcessor<String, Action> implements ActionProcessor<String> {

        @Override
        public void process(String key, AddTodo action) {

        }

        @Override
        public void process(String key, EditTodo action) {

        }

        @Override
        public void process(String key, DeleteTodo action) {

        }

        @Override
        public void process(String key, CompleteAll action) {

        }

        @Override
        public void process(String key, ClearCompleted action) {

        }

        @Override
        public void process(String key, Action action) {
            action.accept(key, this);
        }
    }
}
