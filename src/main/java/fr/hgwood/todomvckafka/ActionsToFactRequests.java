package fr.hgwood.todomvckafka;

import fr.hgwood.todomvckafka.actions.Action;
import fr.hgwood.todomvckafka.actions.DeriveFacts;
import fr.hgwood.todomvckafka.support.kafkastreams.TopicInfo;
import fr.hgwood.todomvckafka.support.kafkastreams.Topology;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import static fr.hgwood.todomvckafka.support.kafkastreams.RandomKey.withRandomKey;

@RequiredArgsConstructor
public class ActionsToFactRequests implements Topology {

    private final TopicInfo<String, Action> actions;
    private final TopicInfo<String, FactRequestTransaction> factRequests;

    @Override
    public void build(KStreamBuilder builder) {
        builder
            .stream(actions.getKeySerde(), actions.getValueSerde(), actions.getName())
            .map((key, action) -> withRandomKey(new FactRequestTransaction(new Transaction<>(action.accept(
                new DeriveFacts())))))
            .to(factRequests.getKeySerde(), factRequests.getValueSerde(), factRequests.getName());
    }
}
