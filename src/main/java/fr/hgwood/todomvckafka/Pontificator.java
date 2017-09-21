package fr.hgwood.todomvckafka;

import fr.hgwood.todomvckafka.actions.Action;
import fr.hgwood.todomvckafka.facts.EntityRetraction;
import fr.hgwood.todomvckafka.facts.Fact;
import fr.hgwood.todomvckafka.support.kafkastreams.TopicInfo;
import fr.hgwood.todomvckafka.support.kafkastreams.Topology;
import lombok.AllArgsConstructor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.function.Supplier;

@AllArgsConstructor
public class Pontificator implements Topology {

    private final TopicInfo<String, Action> actions;
    private final TopicInfo<String, Transaction> transactions;
    private final Supplier<String> transactionIdSupplier;
    private final Supplier<String> entityIdSupplier;

    @Override
    public void build(KStreamBuilder builder) {
        KStream<String, Action> actionKStream =
            builder.stream(actions.getKeySerde(), actions.getValueSerde(), actions.getName());

        KTable<String, Fact> factKStream = actionKStream.flatMap((actionKey, action) -> action
            .deriveFacts(entityIdSupplier)
            .map(fact -> KeyValue.pair(fact.getEntity(), fact)))
            .groupByKey(Serdes.String(), facts)
            .aggregate(() -> true, (entityKey, fact, exists) -> exists && !(fact instanceof EntityRetraction), bool);

        KStream<String, Transaction> transactionKStream =
            actionKStream.map((actionKey, action) -> KeyValue.pair(transactionIdSupplier.get(),
                new Transaction(action.deriveFacts(entityIdSupplier))
            ));

        transactionKStream.to(transactions.getKeySerde(),
            transactions.getValueSerde(),
            transactions.getName()
        );

        //        KTable<String, Boolean> entityExistsKTable = actionKStream
        //            .filter((actionKey, action) -> action instanceof AddTodo || action instanceof DeleteTodo)
        //            .map((actionKey, action) -> action instanceof AddTodo ? KeyValue.pair(entityIdSupplier.get(), true) : KeyValue.pair(((DeleteTodo) action).getId(), false))
        //            .groupByKey(Serdes.String(), bool)
        //            .reduce((exists, action) -> exists && !action)


    }
}
