# Kafka

## Articles 

[What makes Apache Kafka so fast](https://www.freecodecamp.org/news/what-makes-apache-kafka-so-fast-a8d4f94ab145/)

[what happens after a broker is down in a cluster?](https://stackoverflow.com/questions/47132158/what-happens-after-a-broker-is-down-in-a-cluster)

[Kafka: Can consumers read messages before all replicas are in sync?](https://stackoverflow.com/questions/54915336/kafka-can-consumers-read-messages-before-all-replicas-are-in-sync)


## Terminology - Good to Know

__Record__- Base unit handled by the library. It contains: header, key, value, timestamp, topic and partition

__Batch__ - Set of records. Compression happens at batch level

__Request__ - Set of batches. Network communication work based on requests

__Partition__ - Ordered, immutable sequence of records. Topics in Kafka distribute the data across partitions based on the record key

__Partition leader__ - Broker assigned to writes and reads to a particular partition

__Replica__ - Copy of a partition that is being replicated from the leader by another broker

__In sync replica__ - Replica that is in sync with the leader

__Follower__ - Broker performing partition replication from a leader zNode - Unit of data storage in Zookeeper. Ephemeral zNodes exist as long as the Zookeeper session is active

__Watch__ - Mechanism to notify Zookeeper clients on changes of zNodes

## Configuration Types

There are four main ways to update the configuration on your broker:

1.__per-broker__: Configuration on a per-broker basis. This can be updated dynamically, and is typically only used for testing certain configurations. Kafka assumes your brokers have similar configurations and resources especially when it comes to load balancing.

2.__cluster-wide__: Configuration for your entire Kafka cluster. This can also be updated dynamically, changes are applied to all brokers.

3.__read-only__: Configuration that is done in the properties file (server.properties). These changes require a broker restart.

4.__default__: This is the default configuration that is set in Kafka. When you delete a property Kafka reset and use the default.

Dynamic changes will take effect immediately, but will not be updated in the server.properties file. The changes are persisted to ZooKeeper though, and will continue to be effective even if the cluster goes through a rolling restart.

If using configuration management tools, the best practice is to change configurations in code so that they can be version controlled and then applied as a part of a CI/CD pipeline. Using kafka-configs to change configuration dynamically has the benefit of not requiring service restart, but does not allow for the changes to be tracked using a change management system.

## Non-Dynamic Changes

If you are using `server.properties` to update your configuration you will need to perform a rolling restart of your brokers to have them all update. Typically this is used in instances where you would like to use a change management system (Git, Github, etc.) to track any changes to your configuration.

1. Make sure that your cluster is healthy. Make sure that there aren’t any under replicated partitions.
2. Restart one broker at a time waiting for the controller last.
3. Once the restart has taken place before moving on to the next broker make sure that none of the partitions are under replicated.
4. After all brokers have been restarted, restart the controller. Then make sure that the under replicated partitions are down to zero once everything comes back online.

See the following for more information on performing a rolling restart: [Rolling Restart](https://docs.confluent.io/platform/current/kafka/post-deployment.html#rolling-restart)

## Talks

## References

- [Kafka configurations](https://docs.confluent.io/platform/current/installation/configuration/index.html)

- [Dynamic Kafka Configurations](https://docs.confluent.io/platform/current/kafka/dynamic-config.html#)

- [OpenMessaging Benchmark Framework ](https://openmessaging.cloud/docs/benchmarks/)

- [Dynamic broker config change video](https://www.youtube.com/watch?v=imHlHOctGkc)

## Books

## Video courses

[Apache Kafka A-Z with Hands on Learning](https://www.udemy.com/course/apache-kafka-learnkarts/)