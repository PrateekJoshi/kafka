
## Creating a topic

```
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic myTopic --partitions 1 --replication-factor 1
```

## List topics in Kafka 

```
kafka-topics.sh --bootstrap-server localhost:9092 --list
```

## To get topic properties and partition info of a topic

```
kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic myTopic
```
The above command will output the in below format

```
Topic: myTopic	PartitionCount: 1	ReplicationFactor: 1	Configs: segment.bytes=1073741824
	Topic: myTopic	Partition: 0	Leader: 0	Replicas: 0	Isr: 0
```

- 1st line give the topic properties and rest of the below lines gives partition properties.

- __Leader__ : The broker using which reads/writes to Kafka are happening in Kafka for a partion inside a topic.

- __Isr__ : In sync replicas.

## To create a Kafka Producer and Consumer for a Kafka Topic

1. Producer

```
kafka-console-producer.sh --bootstrap-server localhost:9092 --topic myTopic
```

2. Consumer

```
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic myTopic --from-beginning
```

- If we don't specify a consumer group name when creating a consumer, Kafka will iteslef create a new consumer group for this consumer.

- You can specify which consumer group this consumer is associated with by giving CLA `--group myConsumerGroup`.

## Kafka Producers : Writing Messages to Kafka
