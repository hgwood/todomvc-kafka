package fr.hgwood.todomvckafka;

import fr.hgwood.todomvckafka.actions.Action;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class ActionProcessor implements Processor<String, Action> {
    @Override
    public void init(ProcessorContext context) {

    }

    @Override
    public void process(String key, Action value) {

    }

    @Override
    public void punctuate(long timestamp) {

    }

    @Override
    public void close() {

    }
}
