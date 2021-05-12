

## Advanced Kafka topic configuration 

![](./resources/b1.png)

![](./resources/b2.png)

![](./resources/b3.png)

## To list config of a Kafka topic

```
kafka-configs.sh --zookeeper 127.0.0.1:2181 --entity-type topics --entity-name configured-topic --describe 
```

## To list all the config that are available for an entity

```
kafka-configs.sh --zookeeper 127.0.0.1:2181 --entity-type topics --entity-name configured-topic --add-config
```

## To change a config of an entity ( say topic ) 

```
kafka-configs.sh --zookeeper 127.0.0.1:2181 --entity-type topics --entity-name configured-topic --add-config min.insync.replicas=2 --alter
```

## To delete a config 

```
kafka-configs.sh --zookeeper 127.0.0.1:2181 --entity-type topics --entity-name configured-topic --delete-config min.insync.replicas --alter
```

![](./resources/b4.png)

![](./resources/b5.png)

![](./resources/b6.png)

![](./resources/b7.png)

![](./resources/b8.png)

![](./resources/b9.png)

![](./resources/b10.png)

![](./resources/b11.png)

![](./resources/b12.png)

![](./resources/b13.png)

![](./resources/b14.png)

![](./resources/b15.png)

![](./resources/b16.png)

![](./resources/b17.png)

![](./resources/b18.png)

![](./resources/b19.png)