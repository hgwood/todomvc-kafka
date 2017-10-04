package fr.hgwood.todomvckafka;

import fr.hgwood.todomvckafka.actions.Action;
import fr.hgwood.todomvckafka.actions.DeriveFacts;
import fr.hgwood.todomvckafka.actions.InvalidAction;
import fr.hgwood.todomvckafka.facts.DelegatingEntityIdResolver;
import fr.hgwood.todomvckafka.facts.EntityId;
import fr.hgwood.todomvckafka.facts.Fact;
import fr.hgwood.todomvckafka.facts.TemporaryEntityId;
import io.vavr.Function1;
import io.vavr.Value;
import io.vavr.collection.HashSet;
import io.vavr.collection.List;
import io.vavr.collection.Seq;
import io.vavr.collection.Set;
import io.vavr.control.Option;
import io.vavr.control.Try;
import io.vavr.control.Validation;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.UUID;
import java.util.function.Supplier;

@RequiredArgsConstructor
public class ActionProcessor extends AbstractProcessor<String, Action> {

    private final String entityExistsStoreName;
    private final Supplier<String> transactionIdSupplier;
    private final Supplier<String> entityIdSupplier;
    private KeyValueStore<EntityId, EntityId> entityExistsStore;

    @Override
    public void init(ProcessorContext context) {
        super.init(context);
        this.entityExistsStore =
            (KeyValueStore<EntityId, EntityId>) context.getStateStore(entityExistsStoreName);
    }

    @Override
    public void process(String key, Action action) {
        Set<Try<Option<Fact>>> f = action
            .accept(new DeriveFacts())
            .map(factRequest -> factRequest.resolveEntity(new DelegatingEntityIdResolver(
                entityLookup -> Try.of(() -> Option.of(entityExistsStore.get(new EntityId(
                    entityLookup.getValue())))),
                Function1.<TemporaryEntityId, EntityId>of(temporaryEntityId -> {
                    EntityId entityId = new EntityId(UUID.randomUUID().toString());
                    entityExistsStore.put(entityId, entityId);
                    return entityId;
                }).memoized()
            )));
        Validation<Seq<Throwable>, Seq<Fact>> v = Validation.sequence(f
            .map(tryOptionFact -> tryOptionFact.flatMap(Value::toTry))
            .map(tryFact -> Validation.fromEither(tryFact.toEither()))
            .map(validationFact -> validationFact.mapError(List::of)));
        Validation<InvalidAction, Transaction> vv = v.bimap(
            errors -> new InvalidAction(action, errors),
            facts -> new Transaction(HashSet.ofAll(facts))
        );

        this.context().forward(
            transactionIdSupplier.get(),
            vv.isValid() ? vv.get() : vv.getError(),
            vv.isValid() ? 0 : 1
        );
        this.context().commit();


        //        if (action instanceof AddTodo) {
        //            entityExistsStore.put(facts.toList().get(0).getEntity(), true);
        //        } else if (action instanceof DeleteTodo) {
        //            Boolean entityExists = entityExistsStore.get(((DeleteTodo) action).getId());
        //            if (entityExists == Boolean.TRUE) {
        //                entityExistsStore.put(((DeleteTodo) action).getId(), false);
        //            } else {
        //                this.context().forward(key, action, 1);
        //                return;
        //            }
        //        }
        //        this.context().forward(transactionIdSupplier.get(), new Transaction(facts), 0);
        //        this.context().commit();
    }

}
