package org.apache.flink.connector.redisv2;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class RedisConnectorITCase extends RedisTestingClusterAutoStarter {

    @Test
    public void testRedisSinkSetCommand() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(new Configuration());
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, EnvironmentSettings.inStreamingMode());

        // register values table for source
        String dataId =
                TestValuesTableFactory.registerData(
                        Arrays.asList(
                                Row.of("1", "Hello1"),
                                Row.of("1", "Hello2"),
                                Row.of("2", "Hello1"),
                                Row.of("2", "Hello2"),
                                Row.of("2", "Hello3"),
                                Row.of("2", "Hello3"),
                                Row.of("1", "Hello3")
                        )
                );
        tEnv.executeSql(
                "CREATE TABLE source_table ("
                        + " id STRING,"
                        + " name STRING"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'data-id' = '" + dataId + "'"
                        + ")");
        tEnv.executeSql(
                "CREATE TABLE redis_sink ("
                        + " id STRING,"
                        + " name STRING,"
                        + " PRIMARY KEY(id) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'redis-v2',"
                        + " 'mode' = 'standalone',"
                        + " 'nodes' = '"
                        + getRedisHost() + ":" + getRedisPort()
                        + "',"
                        + " 'command' = 'SET'"
                        + ")");
        tEnv.executeSql("INSERT INTO redis_sink(id, name) SELECT id, name FROM source_table").await();

        // start a batch scan job to verify contents in Redis table
        tEnv.executeSql(
                "CREATE TABLE redis_lookup ("
                        + " id STRING,"
                        + " name STRING,"
                        + " PRIMARY KEY(id) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'redis-v2',"
                        + " 'mode' = 'standalone',"
                        + " 'nodes' = '"
                        + getRedisHost() + ":" + getRedisPort()
                        + "',"
                        + " 'command' = 'GET'"
                        + ")");
        Table table = tEnv.sqlQuery(
                "SELECT t.id, name FROM (VALUES ('1', proctime()), ('2', proctime())) t (id, proc_time) " +
                        "INNER JOIN redis_lookup FOR SYSTEM_TIME AS OF t.proc_time ON redis_lookup.id = t.id");
        List<Row> results = CollectionUtil.iteratorToList(table.execute().collect());
        String expected =
                "+I[1, Hello3]\n"
                        + "+I[2, Hello3]";
        TestBaseUtils.compareResultAsText(results, expected);
    }

    @Test
    public void testRedisSinkSetCommandWithNX() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(new Configuration());
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, EnvironmentSettings.inStreamingMode());

        // register values table for source
        String dataId =
                TestValuesTableFactory.registerData(
                        Arrays.asList(
                                Row.of("1", "Hello1"),
                                Row.of("1", "Hello2"),
                                Row.of("2", "Hello1"),
                                Row.of("2", "Hello2"),
                                Row.of("2", "Hello3"),
                                Row.of("2", "Hello3"),
                                Row.of("1", "Hello3")
                        )
                );
        tEnv.executeSql(
                "CREATE TABLE source_table ("
                        + " id STRING,"
                        + " name STRING"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'data-id' = '" + dataId + "'"
                        + ")");
        tEnv.executeSql(
                "CREATE TABLE redis_sink ("
                        + " id STRING,"
                        + " name STRING,"
                        + " PRIMARY KEY(id) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'redis-v2',"
                        + " 'mode' = 'standalone',"
                        + " 'nodes' = '"
                        + getRedisHost() + ":" + getRedisPort()
                        + "',"
                        + " 'command' = 'SETNX'"
                        + ")");
        tEnv.executeSql("INSERT INTO redis_sink(id, name) SELECT id, name FROM source_table").await();

        // start a batch scan job to verify contents in Redis table
        tEnv.executeSql(
                "CREATE TABLE redis_lookup ("
                        + " id STRING,"
                        + " name STRING,"
                        + " PRIMARY KEY(id) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'redis-v2',"
                        + " 'mode' = 'standalone',"
                        + " 'nodes' = '"
                        + getRedisHost() + ":" + getRedisPort()
                        + "',"
                        + " 'command' = 'GET'"
                        + ")");
        Table table = tEnv.sqlQuery(
                "SELECT t.id, name FROM (VALUES ('1', proctime()), ('2', proctime())) t (id, proc_time) " +
                        "INNER JOIN redis_lookup FOR SYSTEM_TIME AS OF t.proc_time ON redis_lookup.id = t.id");
        List<Row> results = CollectionUtil.iteratorToList(table.execute().collect());
        String expected =
                "+I[1, Hello1]\n"
                        + "+I[2, Hello1]";
        TestBaseUtils.compareResultAsText(results, expected);
    }

    @Test
    public void testRedisSinkSetCommandWithExpire() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(new Configuration());
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, EnvironmentSettings.inStreamingMode());

        // register values table for source
        String dataId =
                TestValuesTableFactory.registerData(
                        Arrays.asList(
                                Row.of("1", "Hello", 8L),
                                Row.of("2", "Hello", 4L),
                                Row.of("3", "Hello", -1L)
                        )
                );
        tEnv.executeSql(
                "CREATE TABLE source_table ("
                        + " id STRING,"
                        + " name STRING,"
                        + " expire BIGINT"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'data-id' = '" + dataId + "'"
                        + ")");
        tEnv.executeSql(
                "CREATE TABLE redis_sink ("
                        + " id STRING,"
                        + " name STRING,"
                        + " expire BIGINT METADATA FROM 'expire',"
                        + " PRIMARY KEY(id) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'redis-v2',"
                        + " 'mode' = 'standalone',"
                        + " 'nodes' = '"
                        + getRedisHost() + ":" + getRedisPort()
                        + "',"
                        + " 'command' = 'SET'"
                        + ")");
        tEnv.executeSql("INSERT INTO redis_sink(id, name, expire) SELECT id, name, expire FROM source_table").await();

        // start a batch scan job to verify contents in Redis table
        tEnv.executeSql(
                "CREATE TABLE redis_lookup ("
                        + " id STRING,"
                        + " name STRING,"
                        + " PRIMARY KEY(id) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'redis-v2',"
                        + " 'mode' = 'standalone',"
                        + " 'nodes' = '"
                        + getRedisHost() + ":" + getRedisPort()
                        + "',"
                        + " 'command' = 'GET'"
                        + ")");
        Table table = tEnv.sqlQuery(
                "SELECT t.id, name FROM (VALUES ('1', proctime()), ('2', proctime()), ('3', proctime())) t (id, proc_time) " +
                        "INNER JOIN redis_lookup FOR SYSTEM_TIME AS OF t.proc_time ON redis_lookup.id = t.id");
        List<Row> results = CollectionUtil.iteratorToList(table.execute().collect());
        String expected = "+I[1, Hello]\n+I[2, Hello]\n+I[3, Hello]";
        TestBaseUtils.compareResultAsText(results, expected);

        TimeUnit.SECONDS.sleep(5);
        table = tEnv.sqlQuery(
                "SELECT t.id, name FROM (VALUES ('1', proctime()), ('2', proctime()), ('3', proctime())) t (id, proc_time) " +
                        "INNER JOIN redis_lookup FOR SYSTEM_TIME AS OF t.proc_time ON redis_lookup.id = t.id");
        results = CollectionUtil.iteratorToList(table.execute().collect());
        expected = "+I[1, Hello]\n+I[3, Hello]";
        TestBaseUtils.compareResultAsText(results, expected);

        TimeUnit.SECONDS.sleep(5);
        table = tEnv.sqlQuery(
                "SELECT t.id, name FROM (VALUES ('1', proctime()), ('2', proctime()), ('3', proctime())) t (id, proc_time) " +
                        "INNER JOIN redis_lookup FOR SYSTEM_TIME AS OF t.proc_time ON redis_lookup.id = t.id");
        results = CollectionUtil.iteratorToList(table.execute().collect());
        expected = "+I[3, Hello]";
        TestBaseUtils.compareResultAsText(results, expected);
    }

    @Test
    public void testRedisSinkHSetCommandWithNX() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(new Configuration());
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, EnvironmentSettings.inStreamingMode());

        // register values table for source
        String dataId =
                TestValuesTableFactory.registerData(
                        Arrays.asList(
                                Row.of("k1", "f1", 1),
                                Row.of("k1", "f1", 2),
                                Row.of("k2", "f1", 1),
                                Row.of("k2", "f1", 2),
                                Row.of("k2", "f1", 3),
                                Row.of("k1", "f1", 3)
                        )
                );
        tEnv.executeSql(
                "CREATE TABLE source_table ("
                        + " id STRING,"
                        + " name STRING,"
                        + " age INT"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'data-id' = '" + dataId + "'"
                        + ")");
        tEnv.executeSql(
                "CREATE TABLE redis_sink ("
                        + " id STRING,"
                        + " name STRING,"
                        + " age INT,"
                        + " PRIMARY KEY(id, name) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'redis-v2',"
                        + " 'mode' = 'standalone',"
                        + " 'nodes' = '"
                        + getRedisHost() + ":" + getRedisPort()
                        + "',"
                        + " 'command' = 'HSETNX'"
                        + ")");
        tEnv.executeSql("INSERT INTO redis_sink(id, name, age) SELECT id, name, age FROM source_table").await();

        // start a batch scan job to verify contents in Redis table
        tEnv.executeSql(
                "CREATE TABLE redis_lookup ("
                        + " id STRING,"
                        + " name STRING,"
                        + " age INT,"
                        + " PRIMARY KEY(id, name) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'redis-v2',"
                        + " 'mode' = 'standalone',"
                        + " 'nodes' = '"
                        + getRedisHost() + ":" + getRedisPort()
                        + "',"
                        + " 'command' = 'HGET'"
                        + ")");
        Table table = tEnv.sqlQuery(
                "SELECT t.id, t.name, age FROM (VALUES ('k1', 'f1', proctime()), ('k2', 'f1', proctime())) t (id, name, proc_time) " +
                        "INNER JOIN redis_lookup FOR SYSTEM_TIME AS OF t.proc_time ON redis_lookup.id = t.id AND redis_lookup.name = t.name");
        List<Row> results = CollectionUtil.iteratorToList(table.execute().collect());
        String expected = "+I[k1, f1, 1]\n+I[k2, f1, 1]";
        TestBaseUtils.compareResultAsText(results, expected);
    }

    @Test
    public void testRedisSinkHSetCommandWithExpire() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(new Configuration());
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, EnvironmentSettings.inStreamingMode());

        // register values table for source
        String dataId =
                TestValuesTableFactory.registerData(
                        Arrays.asList(
                                Row.of("k1", "f1", 1, 1L),
                                Row.of("k1", "f2", 2, 8L),
                                Row.of("k2", "f1", 1, 4L),
                                Row.of("k2", "f2", 2, 4L),
                                Row.of("k3", "f1", 1, -1L),
                                Row.of("k3", "f2", 2, -1L)
                        )
                );
        tEnv.executeSql(
                "CREATE TABLE source_table ("
                        + " id STRING,"
                        + " name STRING,"
                        + " age INT,"
                        + " expire BIGINT"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'data-id' = '" + dataId + "'"
                        + ")");
        tEnv.executeSql(
                "CREATE TABLE redis_sink ("
                        + " id STRING,"
                        + " name STRING,"
                        + " age INT,"
                        + " expire BIGINT METADATA FROM 'expire',"
                        + " PRIMARY KEY(id, name) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'redis-v2',"
                        + " 'mode' = 'standalone',"
                        + " 'nodes' = '"
                        + getRedisHost() + ":" + getRedisPort()
                        + "',"
                        + " 'command' = 'HSET'"
                        + ")");
        tEnv.executeSql("INSERT INTO redis_sink(id, name, age, expire) SELECT id, name, age, expire FROM source_table").await();

        // start a batch scan job to verify contents in Redis table
        tEnv.executeSql(
                "CREATE TABLE redis_lookup ("
                        + " id STRING,"
                        + " name STRING,"
                        + " age INT,"
                        + " PRIMARY KEY(id, name) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'redis-v2',"
                        + " 'mode' = 'standalone',"
                        + " 'nodes' = '"
                        + getRedisHost() + ":" + getRedisPort()
                        + "',"
                        + " 'command' = 'HGET'"
                        + ")");
        Table table = tEnv.sqlQuery(
                "SELECT t.id, t.name, age FROM (VALUES " +
                        "('k1', 'f1', proctime()), " +
                        "('k1', 'f2', proctime()), " +
                        "('k2', 'f1', proctime()), " +
                        "('k2', 'f2', proctime()), " +
                        "('k3', 'f1', proctime())," +
                        "('k3', 'f2', proctime())" +
                        ") t (id, name, proc_time) " +
                        "INNER JOIN redis_lookup FOR SYSTEM_TIME AS OF t.proc_time ON redis_lookup.id = t.id AND redis_lookup.name = t.name");
        List<Row> results = CollectionUtil.iteratorToList(table.execute().collect());
        String expected = "+I[k1, f1, 1]\n+I[k1, f2, 2]\n+I[k2, f1, 1]\n+I[k2, f2, 2]\n+I[k3, f1, 1]\n+I[k3, f2, 2]";
        TestBaseUtils.compareResultAsText(results, expected);

        TimeUnit.SECONDS.sleep(5);
        table = tEnv.sqlQuery(
                "SELECT t.id, t.name, age FROM (VALUES " +
                        "('k1', 'f1', proctime()), " +
                        "('k1', 'f2', proctime()), " +
                        "('k2', 'f1', proctime()), " +
                        "('k2', 'f2', proctime()), " +
                        "('k3', 'f1', proctime())," +
                        "('k3', 'f2', proctime())" +
                        ") t (id, name, proc_time) " +
                        "INNER JOIN redis_lookup FOR SYSTEM_TIME AS OF t.proc_time ON redis_lookup.id = t.id AND redis_lookup.name = t.name");
        results = CollectionUtil.iteratorToList(table.execute().collect());
        expected = "+I[k1, f1, 1]\n+I[k1, f2, 2]\n+I[k3, f1, 1]\n+I[k3, f2, 2]";
        TestBaseUtils.compareResultAsText(results, expected);

        TimeUnit.SECONDS.sleep(5);
        table = tEnv.sqlQuery(
                "SELECT t.id, t.name, age FROM (VALUES " +
                        "('k1', 'f1', proctime()), " +
                        "('k1', 'f2', proctime()), " +
                        "('k2', 'f1', proctime()), " +
                        "('k2', 'f2', proctime()), " +
                        "('k3', 'f1', proctime())," +
                        "('k3', 'f2', proctime())" +
                        ") t (id, name, proc_time) " +
                        "INNER JOIN redis_lookup FOR SYSTEM_TIME AS OF t.proc_time ON redis_lookup.id = t.id AND redis_lookup.name = t.name");
        results = CollectionUtil.iteratorToList(table.execute().collect());
        expected = "+I[k3, f1, 1]\n+I[k3, f2, 2]";
        TestBaseUtils.compareResultAsText(results, expected);
    }

    @Test
    public void testRedisSinkHSetCommand() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(new Configuration());
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, EnvironmentSettings.inStreamingMode());

        // register values table for source
        String dataId =
                TestValuesTableFactory.registerData(
                        Arrays.asList(
                                Row.of("k1", "f1", 1),
                                Row.of("k2", "f1", 1),
                                Row.of("k2", "f2", 2),
                                Row.of("k2", "f1", 3),
                                Row.of("k2", "f3", 3),
                                Row.of("k1", "f2", 3)
                        )
                );
        tEnv.executeSql(
                "CREATE TABLE source_table ("
                        + " id STRING,"
                        + " name STRING,"
                        + " age INT"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'data-id' = '" + dataId + "'"
                        + ")");
        tEnv.executeSql(
                "CREATE TABLE redis_sink ("
                        + " id STRING,"
                        + " name STRING,"
                        + " age INT,"
                        + " PRIMARY KEY(id, name) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'redis-v2',"
                        + " 'mode' = 'standalone',"
                        + " 'nodes' = '"
                        + getRedisHost() + ":" + getRedisPort()
                        + "',"
                        + " 'command' = 'HSET'"
                        + ")");
        tEnv.executeSql("INSERT INTO redis_sink(id, name, age) SELECT id, name, age FROM source_table").await();

        // start a batch scan job to verify contents in Redis table
        tEnv.executeSql(
                "CREATE TABLE redis_lookup ("
                        + " id STRING,"
                        + " name STRING,"
                        + " age INT,"
                        + " PRIMARY KEY(id, name) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'redis-v2',"
                        + " 'mode' = 'standalone',"
                        + " 'nodes' = '"
                        + getRedisHost() + ":" + getRedisPort()
                        + "',"
                        + " 'command' = 'HGET'"
                        + ")");
        Table table = tEnv.sqlQuery(
                "SELECT t.id AS id, t.name AS name, age FROM (VALUES " +
                        "('k1', 'f1', proctime()), " +
                        "('k1', 'f2', proctime()), " +
                        "('k2', 'f1', proctime()), " +
                        "('k2', 'f2', proctime()), " +
                        "('k2', 'f3', proctime()) " +
                        ") t (id, name, proc_time) " +
                        "INNER JOIN redis_lookup FOR SYSTEM_TIME AS OF t.proc_time ON redis_lookup.id = t.id AND redis_lookup.name = t.name");
        List<Row> results = CollectionUtil.iteratorToList(table.execute().collect());
        String expected = "+I[k1, f1, 1]\n+I[k1, f2, 3]\n+I[k2, f1, 3]\n+I[k2, f2, 2]\n+I[k2, f3, 3]";
        TestBaseUtils.compareResultAsText(results, expected);
    }

    @Test
    public void testRedisSinkIncrbyCommand() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(new Configuration());
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, EnvironmentSettings.inStreamingMode());

        // register values table for source
        String dataId =
                TestValuesTableFactory.registerData(
                        Arrays.asList(
                                Row.of("inc1", 1L),
                                Row.of("inc2", 1L),
                                Row.of("inc2", 2L),
                                Row.of("inc2", 3L),
                                Row.of("inc1", 3L)
                        )
                );
        tEnv.executeSql(
                "CREATE TABLE source_table ("
                        + " id STRING,"
                        + " amount BIGINT"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'data-id' = '" + dataId + "'"
                        + ")");
        tEnv.executeSql(
                "CREATE TABLE redis_sink ("
                        + " id STRING,"
                        + " amount BIGINT,"
                        + " PRIMARY KEY(id) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'redis-v2',"
                        + " 'mode' = 'standalone',"
                        + " 'nodes' = '"
                        + getRedisHost() + ":" + getRedisPort()
                        + "',"
                        + " 'command' = 'INCRBY'"
                        + ")");
        tEnv.executeSql("INSERT INTO redis_sink(id, amount) SELECT id, amount FROM source_table").await();

        // start a batch scan job to verify contents in Redis table
        tEnv.executeSql(
                "CREATE TABLE redis_lookup ("
                        + " id STRING,"
                        + " amount BIGINT,"
                        + " PRIMARY KEY(id) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'redis-v2',"
                        + " 'mode' = 'standalone',"
                        + " 'nodes' = '"
                        + getRedisHost() + ":" + getRedisPort()
                        + "',"
                        + " 'command' = 'GET'"
                        + ")");
        Table table = tEnv.sqlQuery(
                "SELECT t.id AS id, amount FROM (VALUES " +
                        "('inc1', proctime()), " +
                        "('inc2', proctime()) " +
                        ") t (id, proc_time) " +
                        "INNER JOIN redis_lookup FOR SYSTEM_TIME AS OF t.proc_time ON redis_lookup.id = t.id");
        List<Row> results = CollectionUtil.iteratorToList(table.execute().collect());
        String expected = "+I[inc1, 4]\n+I[inc2, 6]";
        TestBaseUtils.compareResultAsText(results, expected);
    }

    @Test
    public void testRedisSinkIncrbyCommandWithExpire() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(new Configuration());
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, EnvironmentSettings.inStreamingMode());

        // register values table for source
        String dataId =
                TestValuesTableFactory.registerData(
                        Arrays.asList(
                                Row.of("inc_expire1", 1L, -1L),
                                Row.of("inc_expire2", 1L, 10L),
                                Row.of("inc_expire2", 2L, 10L),
                                Row.of("inc_expire2", 3L, 10L),
                                Row.of("inc_expire1", 3L, 4L)
                        )
                );
        tEnv.executeSql(
                "CREATE TABLE source_table ("
                        + " id STRING,"
                        + " amount BIGINT,"
                        + " expire BIGINT"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'data-id' = '" + dataId + "'"
                        + ")");
        tEnv.executeSql(
                "CREATE TABLE redis_sink ("
                        + " id STRING,"
                        + " amount BIGINT,"
                        + " expire BIGINT METADATA FROM 'expire',"
                        + " PRIMARY KEY(id) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'redis-v2',"
                        + " 'mode' = 'standalone',"
                        + " 'nodes' = '"
                        + getRedisHost() + ":" + getRedisPort()
                        + "',"
                        + " 'command' = 'INCRBY'"
                        + ")");
        tEnv.executeSql("INSERT INTO redis_sink(id, amount, expire) SELECT id, amount, expire FROM source_table").await();

        // start a batch scan job to verify contents in Redis table
        tEnv.executeSql(
                "CREATE TABLE redis_lookup ("
                        + " id STRING,"
                        + " amount BIGINT,"
                        + " PRIMARY KEY(id) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'redis-v2',"
                        + " 'mode' = 'standalone',"
                        + " 'nodes' = '"
                        + getRedisHost() + ":" + getRedisPort()
                        + "',"
                        + " 'command' = 'GET'"
                        + ")");
        Table table = tEnv.sqlQuery(
                "SELECT t.id AS id, amount FROM (VALUES " +
                        "('inc_expire1', proctime()), " +
                        "('inc_expire2', proctime()) " +
                        ") t (id, proc_time) " +
                        "INNER JOIN redis_lookup FOR SYSTEM_TIME AS OF t.proc_time ON redis_lookup.id = t.id");
        List<Row> results = CollectionUtil.iteratorToList(table.execute().collect());
        String expected = "+I[inc_expire1, 4]\n+I[inc_expire2, 6]";
        TestBaseUtils.compareResultAsText(results, expected);

        TimeUnit.SECONDS.sleep(5);
        table = tEnv.sqlQuery(
                "SELECT t.id AS id, amount FROM (VALUES " +
                        "('inc_expire1', proctime()), " +
                        "('inc_expire2', proctime()) " +
                        ") t (id, proc_time) " +
                        "INNER JOIN redis_lookup FOR SYSTEM_TIME AS OF t.proc_time ON redis_lookup.id = t.id");
        results = CollectionUtil.iteratorToList(table.execute().collect());
        expected = "+I[inc_expire2, 6]";
        TestBaseUtils.compareResultAsText(results, expected);
    }

    @ParameterizedTest
    @EnumSource(Caching.class)
    void testRedisLookupTableSource(Caching caching) throws Exception {
        verifyRedisLookupJoin(caching, false);
    }

    @ParameterizedTest
    @EnumSource(Caching.class)
    void testRedisAsyncLookupTableSource(Caching caching) throws Exception {
        verifyRedisLookupJoin(caching, true);
    }

    private void verifyRedisLookupJoin(Caching caching, boolean async) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(new Configuration());
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, EnvironmentSettings.inStreamingMode());

        // register values table for source
        String dataId =
                TestValuesTableFactory.registerData(
                        Arrays.asList(
                                Row.of("1", "Hello1"),
                                Row.of("2", "Hello2"),
                                Row.of("3", "Hello3")
                        )
                );
        tEnv.executeSql(
                "CREATE TABLE source_table ("
                        + " id STRING,"
                        + " name STRING,"
                        + " PRIMARY KEY (id) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'data-id' = '" + dataId + "'"
                        + ")");
        tEnv.executeSql(
                "CREATE TABLE redis_sink ("
                        + " id STRING,"
                        + " name STRING,"
                        + " PRIMARY KEY(id) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'redis-v2',"
                        + " 'mode' = 'standalone',"
                        + " 'nodes' = '"
                        + getRedisHost() + ":" + getRedisPort()
                        + "',"
                        + " 'command' = 'SET'"
                        + ")");
        tEnv.executeSql("INSERT INTO redis_sink(id, name) SELECT id, name FROM source_table").await();

        String cacheOptions = "";
        if (caching == Caching.ENABLE_CACHE) {
            cacheOptions =
                    ","
                            + String.join(
                            ",",
                            Arrays.asList(
                                    "'lookup.cache' = 'PARTIAL'",
                                    "'lookup.partial-cache.max-rows' = '1000'",
                                    "'lookup.partial-cache.expire-after-write' = '10min'"));
        }

        tEnv.executeSql(
                "CREATE TABLE redis_lookup ("
                        + " id STRING,"
                        + " name STRING,"
                        + " PRIMARY KEY(id) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'redis-v2',"
                        + " 'mode' = 'standalone',"
                        + " 'nodes' = '"
                        + getRedisHost() + ":" + getRedisPort()
                        + "',"
                        + " 'command' = 'GET',"
                        + " 'lookup.async' = '"
                        + async
                        + "'"
                        + cacheOptions
                        + ")");
        Table table = tEnv.sqlQuery(
                "SELECT t.id, name FROM (VALUES ('1', proctime()), ('2', proctime()), ('3', proctime())) t (id, proc_time) " +
                        "INNER JOIN redis_lookup FOR SYSTEM_TIME AS OF t.proc_time ON redis_lookup.id = t.id");
        List<Row> results = CollectionUtil.iteratorToList(table.execute().collect());
        String expected =
                "+I[1, Hello1]\n"
                        + "+I[2, Hello2]\n"
                        + "+I[3, Hello3]";
        TestBaseUtils.compareResultAsText(results, expected);
    }

    private enum Caching {
        ENABLE_CACHE,
        DISABLE_CACHE
    }
}
