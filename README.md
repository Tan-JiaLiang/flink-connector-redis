# Flink Redis Connector
A redis connector for apache flink.

# Building from Source
```
git clone https://github.com/Tan-JiaLiang/flink-connector-redis.git
cd flink-connector-redis-v2
mvn clean package -DskipTests
```

# Example
```sql
CREATE TABLE source_table(
  id INT,
  name STRING
) WITH (
  'connector' = 'datagen',
  'fields.id.min' = '0',
  'fields.id.max' = '10',
  'fields.name.length' = '10',
  'rows-per-second' = '30'
);

CREATE TABLE redis_sink (
    id STRING,
    name STRING,
    expire BIGINT METADATA FROM 'expire',
    PRIMARY KEY(id) NOT ENFORCED
) WITH (
    'connector' = 'redis-v2',
    'mode' = 'cluster',
    'nodes' = 'localhost:6379',
    'command' = 'SET'
);

INSERT INTO redis_sink(id, name, expire)
SELECT CONCAT('REDIS_V2_TEST::', CAST(id AS STRING)), name, 300 FROM source_table;
```

Available Metadata
------------------

The following connector metadata can be accessed as metadata columns in a table definition.

The `R/W` column defines whether a metadata field is readable (`R`) and/or writable (`W`).
Read-only columns must be declared `VIRTUAL` to exclude them during an `INSERT INTO` operation.

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 25%">Key</th>
      <th class="text-center" style="width: 30%">Data Type</th>
      <th class="text-center" style="width: 40%">Description</th>
      <th class="text-center" style="width: 5%">R/W</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><code>expire</code></td>
      <td><code>BIGINT NOT NULL</code></td>
      <td>Time-to-live for the Redis key, in seconds.</td>
      <td><code>W</code></td>
    </tr>
    </tbody>
</table>

# Connector Options
<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left" style="width: 25%">Option</th>
        <th class="text-center" style="width: 8%">Required</th>
        <th class="text-center" style="width: 7%">Default</th>
        <th class="text-center" style="width: 10%">Type</th>
        <th class="text-center" style="width: 42%">Description</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>connector</h5></td>
      <td>required</td>
      <td>(none)</td>
      <td>String</td>
      <td><code>redis-v2</code></td>
    </tr>
    <tr>
      <td><h5>mode</h5></td>
      <td>required</td>
      <td>(none)</td>
      <td><p>Enum</p>Possible values: standalone, cluster</td>
      <td>Redis deploy mode, support standalone and cluster.</td>
    </tr>
    <tr>
      <td><h5>command</h5></td>
      <td>required</td>
      <td>(none)</td>
      <td>String</td>
      <td>Redis command, support <code>SET</code>,<code>SETNX</code>,<code>HSET</code>,<code>HSETNX</code>,<code>INCRBY</code> for sink, <code>GET</code>,<code>HGET</code> for lookup.</td>
    </tr>
    <tr>
      <td><h5>nodes</h5></td>
      <td>required</td>
      <td>(none)</td>
      <td>String</td>
      <td>Redis nodes, example: <code>192.168.1.1:6379,192.168.1.2:6379,192.168.1.3:6379</code> for cluster, <code>192.168.1.1:6379</code> for standalone.</td>
    </tr>
    <tr>
      <td><h5>io.pool.size</h5></td>
      <td>optional</td>
      <td>1</td>
      <td>Integer</td>
      <td>The thread pool size (number of threads to use) for I/O operations.</td>
    </tr>
    <tr>
      <td><h5>computation.pool.size</h5></td>
      <td>optional</td>
      <td>1</td>
      <td>Integer</td>
      <td>The thread pool size (number of threads to use) for computation operations.</td>
    </tr>
    <tr>
      <td><h5>timeout</h5></td>
      <td>optional</td>
      <td>30s</td>
      <td>Duration</td>
      <td>The timeout for connecting to Redis server.</td>
    </tr>
    <tr>
      <td><h5>password</h5></td>
      <td>optional</td>
      <td>(none)</td>
      <td>String</td>
      <td>The password for connecting to Redis server.</td>
    </tr>
    <tr>
      <td><h5>sink.buffer-flush.max-rows</h5></td>
      <td>optional</td>
      <td>1000</td>
      <td>Integer</td>
      <td>Writing option, maximum number of rows to buffer for each writing request. This can improve performance for writing data to Redis, but may increase the latency. Can be set to <code>0</code> to disable it.</td>
    </tr>
    <tr>
      <td><h5>sink.buffer-flush.interval</h5></td>
      <td>optional</td>
      <td>1000</td>
      <td>Integer</td>
      <td>Writing option, the interval to flush any buffered rows. This can improve performance for writing data to Redis, but may increase the latency. Can be set to <code>0</code> to disable it.</td>
    </tr>
    <tr>
      <td><h5>sink.parallelism</h5></td>
      <td>optional</td>
      <td>(none)</td>
      <td>Integer</td>
      <td>Defines the parallelism of the Redis sink operator. By default, the parallelism is determined by the framework using the same parallelism of the upstream chained operator.</td>
    </tr>
    <tr>
      <td><h5>lookup.async</h5></td>
      <td>optional</td>
      <td>false</td>
      <td>Boolean</td>
      <td>Whether async lookup are enabled. If true, the lookup will be async.</td>
    </tr>
    <tr>
      <td><h5>lookup.cache</h5></td>
      <td>optional</td>
      <td>NONE</td>
      <td><p>Enum</p>Possible values: NONE, PARTIAL</td>
      <td>The cache strategy for the lookup table. Currently supports NONE (no caching) and PARTIAL (caching entries on lookup operation in external database).</td>
    </tr>
    <tr>
      <td><h5>lookup.partial-cache.max-rows</h5></td>
      <td>optional</td>
      <td>(none)</td>
      <td>Long</td>
      <td>The max number of rows of lookup cache, over this value, the oldest rows will be expired. 
        "lookup.cache" must be set to "PARTIAL" to use this option.</td>
    </tr>
    <tr>
      <td><h5>lookup.partial-cache.expire-after-write</h5></td>
      <td>optional</td>
      <td>(none)</td>
      <td>Duration</td>
      <td>The max time to live for each rows in lookup cache after writing into the cache
        "lookup.cache" must be set to "PARTIAL" to use this option. </td>
    </tr>
    <tr>
      <td><h5>lookup.partial-cache.expire-after-access</h5></td>
      <td>optional</td>
      <td>(none)</td>
      <td>Duration</td>
      <td>The max time to live for each rows in lookup cache after accessing the entry in the cache.
      "lookup.cache" must be set to "PARTIAL" to use this option. </td>
    </tr>
    <tr>
      <td><h5>lookup.partial-cache.caching-missing-key</h5></td>
      <td>optional</td>
      <td>true</td>
      <td>Boolean</td>
      <td>Whether to store an empty value into the cache if the lookup key doesn't match any rows in the table. 
        "lookup.cache" must be set to "PARTIAL" to use this option.</td>
    </tr>
    <tr>
      <td><h5>lookup.max-retries</h5></td>
      <td>optional</td>
      <td>3</td>
      <td>Integer</td>
      <td>The max retry times if lookup database failed.</td>
    </tr>
    <tr>
      <td><h5>lookup.retry.backoff</h5></td>
      <td>optional</td>
      <td>1s</td>
      <td>Duration</td>
      <td>The retry backoff interval for retry lookup.</td>
    </tr>
    </tbody>
</table>

# Data Type Mapping
<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left">Flink SQL type</th>
        <th class="text-left">Redis conversion</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td><code>CHAR / VARCHAR / STRING</code></td>
      <td><code>STRING</code></td>
    </tr>
    <tr>
      <td><code>INT</code></td>
      <td><code>INT</code></td>
    <tr>
      <td><code>BIGINT</code></td>
      <td><code>BIGINT</code></td>
    </tr>
    <tr>
      <td><code>BOOLEAN</code></td>
      <td>Not supported.</td>
    </tr>
    <tr>
      <td><code>BINARY / VARBINARY</code></td>
      <td>Not supported.</td>
    </tr>
    <tr>
      <td><code>DECIMAL</code></td>
      <td>Not supported.</td>
    </tr>
    <tr>
      <td><code>TINYINT</code></td>
      <td>Not supported.</td>
    </tr>
    <tr>
      <td><code>SMALLINT</code></td>
      <td>Not supported.</td>
    </tr>
    <tr>
      <td><code>FLOAT</code></td>
      <td>Not supported.</td>
    </tr>
    <tr>
      <td><code>DOUBLE</code></td>
      <td>Not supported.</td>
    </tr>
    <tr>
      <td><code>DATE</code></td>
      <td>Not supported.</td>
    </tr>
    <tr>
      <td><code>TIME</code></td>
      <td>Not supported.</td>
    </tr>
    <tr>
      <td><code>TIMESTAMP</code></td>
      <td>Not supported.</td>
    </tr>
    <tr>
      <td><code>ARRAY</code></td>
      <td>Not supported.</td>
    </tr>
    <tr>
      <td><code>MAP / MULTISET</code></td>
      <td>Not supported.</td>
    </tr>
    <tr>
      <td><code>ROW</code></td>
      <td>Not supported.</td>
    </tr>
    </tbody>
</table>
