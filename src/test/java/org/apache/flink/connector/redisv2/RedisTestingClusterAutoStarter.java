package org.apache.flink.connector.redisv2;

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;

@SuppressWarnings("all")
public class RedisTestingClusterAutoStarter {

    private static final Logger LOG = LoggerFactory.getLogger(RedisTestingClusterAutoStarter.class);

    public static final String REDIS_IMAGE = "redis:5.0.14";
    public static final int REDIS_PORT = 6379;

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_EXTENSION =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(4)
                            .build());

    private static final GenericContainer<?> CONTAINER =
            new GenericContainer<>(REDIS_IMAGE).withExposedPorts(REDIS_PORT);

    @BeforeAll
    public static void setUp() throws Exception {
        LOG.info("redis container: starting");
        CONTAINER.start();
        LOG.info("redis container: started with expose port: {}", REDIS_PORT);
    }

    @AfterAll
    public static void tearDown() throws Exception {
        LOG.info("redis container: shutting down");
        CONTAINER.stop();
        LOG.info("redis container: down");
    }

    public String getRedisHost() {
        return CONTAINER.getHost();
    }

    public int getRedisPort() {
        return CONTAINER.getMappedPort(REDIS_PORT);
    }
}
