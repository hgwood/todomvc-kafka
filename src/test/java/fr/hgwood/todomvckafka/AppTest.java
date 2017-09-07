package fr.hgwood.todomvckafka;


import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.DockerComposeContainer;

import java.io.File;

public class AppTest {

    public static final int ZOOKEEPER_PORT = 2181;
    public static final int KAFKA_PORT = 9092;

    @ClassRule public static final DockerComposeContainer dockerComposeContainer =
        new DockerComposeContainer(
            new File("src/test/resources/compose-test.yml"))
            .withLocalCompose(true)
            .withExposedService("zookeeper_1", ZOOKEEPER_PORT)
            .withExposedService("kafka_1", KAFKA_PORT);

    @Test
    public void dockerComposeContainerStarts() {

    }

}
