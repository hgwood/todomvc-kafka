package fr.hgwood.todomvckafka.actions;

import fr.hgwood.todomvckafka.actions.todoitem.AddTodo;
import fr.hgwood.todomvckafka.actions.todoitem.DeleteTodo;

public interface ActionVisitor<T> {
    T visit(AddTodo action);

    T visit(DeleteTodo action);
}
