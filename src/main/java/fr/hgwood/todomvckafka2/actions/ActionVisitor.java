package fr.hgwood.todomvckafka2.actions;

public interface ActionVisitor<T> {
    T visit(AddTodo action);

    T visit(DeleteTodo action);
}
