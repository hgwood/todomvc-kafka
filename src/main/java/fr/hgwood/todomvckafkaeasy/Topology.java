package fr.hgwood.todomvckafkaeasy;

import com.fasterxml.jackson.databind.ObjectMapper;
import fr.hgwood.todomvckafka2.support.json.JsonSerde;
import fr.hgwood.todomvckafka2.support.kafkastreams.TopicInfo;
import fr.hgwood.todomvckafkaeasy.actions.*;
import io.vavr.collection.List;
import io.vavr.jackson.datatype.VavrModule;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.UUID;

public class Topology implements ActionReducer {

    private static final ObjectMapper OBJECT_MAPPER =
        new ObjectMapper().registerModule(new VavrModule());

    private static final TopicInfo<String, Action> ACTIONS = new TopicInfo<>("test-actions-topic",
        Serdes.String(),
        new JsonSerde<>(OBJECT_MAPPER, Action.class)
    );

    private static final TopicInfo<String, State> STATES = new TopicInfo<>("test-states-topic",
        Serdes.String(),
        new JsonSerde<>(OBJECT_MAPPER, State.class)
    );

    public static void main(String[] args) {

    }

    public void buildTopology(KStreamBuilder builder) {
        builder
            .stream(ACTIONS.getKeySerde(), ACTIONS.getValueSerde(), ACTIONS.getName())
            .groupBy((key, action) -> null)
            .aggregate(() -> new State(List.empty()),
                ((key, action, state) -> action.accept(this, state)),
                STATES.getValueSerde()
            )
            .toStream()
            .selectKey((key, state) -> UUID.randomUUID().toString())
            .to(STATES.getKeySerde(), STATES.getValueSerde(), STATES.getName());
        
    }

    @Override
    public State apply(AddTodo action, State state) {
        return new State(state
            .getTodoItems()
            .append(new State.TodoItem(action.getText(), false, UUID.randomUUID())));
    }

    @Override
    public State apply(EditTodo action, State state) {
        return new State(state
            .getTodoItems()
            .map(todoItem -> todoItem
                .getId()
                .equals(action.getId()) ? new State.TodoItem(action.getText(),
                todoItem.isCompleted(),
                todoItem.getId()
            ) : todoItem));
    }

    @Override
    public State apply(DeleteTodo action, State state) {
        return new State(state
            .getTodoItems()
            .filter(todoItem -> !todoItem.getId().equals(action.getId())));
    }

    @Override
    public State apply(CompleteAll action, State state) {
        boolean areAllMarked = state.getTodoItems().forAll(todoItem -> todoItem.isCompleted());
        return new State(state
            .getTodoItems()
            .map(todoItem -> new State.TodoItem(todoItem.getText(),
                !areAllMarked,
                todoItem.getId()
            )));
    }

    @Override
    public State apply(ClearCompleted action, State state) {
        return new State(state.getTodoItems().filter(todoItem -> !todoItem.isCompleted()));
    }
}
