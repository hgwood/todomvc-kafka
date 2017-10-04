package fr.hgwood.todomvckafka;

import fr.hgwood.todomvckafka.facts.Fact;
import fr.hgwood.todomvckafka.support.kafkastreams.TopicInfo;
import fr.hgwood.todomvckafka.support.kafkastreams.Topology;
import io.vavr.collection.HashMap;
import io.vavr.collection.Map;
import io.vavr.collection.Set;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import static fr.hgwood.todomvckafka.support.kafkastreams.ConvertFromVavr.toKeyValues;

public class EntityGatherer implements Topology {

    private final TopicInfo<String, Transaction> transactions;
    private final TopicInfo<String, Map> entities;

    public EntityGatherer(
        TopicInfo<String, Transaction> transactions, TopicInfo<String, Map> entities
    ) {
        this.transactions = transactions;
        this.entities = entities;
    }

    @Override
    public void build(KStreamBuilder builder) {
        builder
            .stream(transactions.getKeySerde(),
                transactions.getValueSerde(),
                transactions.getName()
            )
            .mapValues(transaction -> this.mergeFacts(transaction.getFacts()))
            .flatMap((transactionKey, entities) -> toKeyValues(entities))
            .to(entities.getKeySerde(), entities.getValueSerde(), entities.getName());
    }

    private Map<String, Map> mergeFacts(Set<Fact> facts) {
        return facts
            .groupBy(fact -> fact.getEntity().getValue())
            .mapValues(entityFacts -> entityFacts.foldLeft(
                (Map<String, Object>) HashMap.<String, Object>empty(),
                (fields, fact) -> fact.apply(fields)
            ))
            .mapValues(fields -> fields.isEmpty() ? null : fields);
    }

}
