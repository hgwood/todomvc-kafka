package fr.hgwood.todomvckafka.facts;

public interface FactRequestVisitor<T> {
    T visit(AssertionRequest factRequest);

    T visit(AccretionRequest factRequest);

    T visit(EntityRetractionRequest factRequest);
}
