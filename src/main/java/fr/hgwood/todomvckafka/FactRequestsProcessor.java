package fr.hgwood.todomvckafka;

import fr.hgwood.todomvckafka.facts.*;
import io.vavr.collection.HashSet;
import io.vavr.collection.Seq;
import io.vavr.collection.Set;
import io.vavr.control.Option;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

@RequiredArgsConstructor
public class FactRequestsProcessor extends AbstractProcessor<String, FactRequestTransaction> {

    private final String entityExistsStoreName;
    private KeyValueStore<EntityId, EntityId> entityExistsStore;

    @Override
    public void init(ProcessorContext context) {
        super.init(context);
        this.entityExistsStore =
            (KeyValueStore<EntityId, EntityId>) context.getStateStore(entityExistsStoreName);
    }

    @Override
    public void process(String key, FactRequestTransaction value) {
        FactRequestVisitor<Option<Fact>> visitor = new EntityExistenceBuilder2(this.entityExistsStore);
        Set<Option<Fact>> facts = value.getTransaction().getFacts().map(factRequest -> factRequest.accept(visitor));
        Option<Seq<Fact>> result = Option.sequence(facts);
        //Option<Transaction<Fact>> transaction = result.map(facts1 -> new Transaction<>(HashSet.ofAll(facts1)));
        if (result.isDefined()) {
            FactTransaction transaction = new FactTransaction(new Transaction<>(HashSet.ofAll(result.get())));
            this.context().forward(key, transaction, 0);
        } else {
            this.context().forward(key, value, 1);
        }
    }
}
