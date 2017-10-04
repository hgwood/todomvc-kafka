package fr.hgwood.todomvckafka;

import fr.hgwood.todomvckafka.actions.Action;
import fr.hgwood.todomvckafka.actions.DeriveFacts;
import fr.hgwood.todomvckafka.actions.InvalidAction;
import fr.hgwood.todomvckafka.facts.*;
import io.vavr.Function1;
import io.vavr.collection.Set;
import io.vavr.control.Either;
import io.vavr.control.Option;
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
        Set<Either<NoSuchEntity, Fact>> f = action
            .accept(new DeriveFacts())
            .map(factRequest -> factRequest.resolveEntity(new DelegatingEntityIdResolver(
                entityLookup -> Option
                    .of(entityExistsStore.get(new EntityId(entityLookup.getValue())))
                    .toEither(new NoSuchEntity(entityLookup.getValue())),
                Function1.<TemporaryEntityId, EntityId>of(temporaryEntityId -> {
                    EntityId entityId = new EntityId(UUID.randomUUID().toString());
                    entityExistsStore.put(entityId, entityId);
                    return entityId;
                }).memoized()
            )));

        Option<NoSuchEntity> firstError =
            f.find(maybeFact -> maybeFact.isLeft()).map(e -> e.getLeft());
        if (firstError.isDefined()) {
            this.context().forward(key, new InvalidAction(action, firstError.get()), 1);
        } else {
            this
                .context()
                .forward(transactionIdSupplier.get(), new Transaction(f.map(Either::get)), 0);
        }
    }

}
