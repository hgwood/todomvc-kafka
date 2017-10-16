package fr.hgwood.todomvckafka2.reducers;

import fr.hgwood.todomvckafka2.actions.Action;
import fr.hgwood.todomvckafka2.support.kafkastreams.TopicInfo;
import fr.hgwood.todomvckafka2.support.kafkastreams.Topology;
import io.vavr.collection.Set;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

@RequiredArgsConstructor
public class Transactor2 implements Topology {

    private final TopicInfo<String, Action> actionsInfo;
    private final TopicInfo<View, String> viewPingsInfo;

    @Override
    public void build(KStreamBuilder builder) {


        KStream<String, Action> actions = builder.stream(actionsInfo.getKeySerde(),
            actionsInfo.getValueSerde(),
            actionsInfo.getName()
        );

        KTable<View, String> lockers = actions
            .flatMap((actionKey, action) -> getWrittenViews(action).map(view -> KeyValue.pair(
                view,
                actionKey
            )))
            .groupByKey()
            .reduce((previousLocker, newLocker) -> newLocker);

        KStream<View, String> viewPings = builder.stream(viewPingsInfo.getKeySerde(), viewPingsInfo.getValueSerde(), viewPingsInfo.getName());
        KStream<View, String> effectiveLockers = viewPings.join(lockers, (ping, locker) -> {
            if (ping.equals(locker)) {
                return null;
            } else {
                return locker;
            }
        });
        KStream<View, String> unlockings = effectiveLockers
            .filter((view, locker) -> locker == null);

        actions.flatMap((actionKey, action) -> getReadViews(action).map(view -> KeyValue.pair(
            view,
            actionKey
        ))).join(unlockings, (currentAction, lockingAction) -> {

        });
    }

    private Set<View> getWrittenViews(Action action) {
        return null;
    }

    private Set<View> getReadViews(Action action) {
        return null;
    }

    private static class View {

    }
}
