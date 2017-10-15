package fr.hgwood.todomvckafka2.actions;

public interface ActionProcessor<K> {
    void process(K key, AddTodo action);

    void process(K key, DeleteTodo action);
}
