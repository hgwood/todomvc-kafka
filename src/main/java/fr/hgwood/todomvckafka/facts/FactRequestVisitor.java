package fr.hgwood.todomvckafka.facts;

public interface FactRequestVisitor<T> {
    <R> T visit(AssertionRequest<R> factRequest);

    <R> T visit(AccretionRequest<R> factRequest);

    T visit(EntityRetractionRequest factRequest);
}
