# 数据接入部分

自己写的类：

- kafka/ValidatedConsumer
- kafka/ValidatedProducer
- kafka/ValidatedStream

  
- redis/RedisConf
- redis/RedisPublisher
- redis/RedisSubscriber
  

- main/ConsumerMain
- main/ProducerMain
- main/Stream


包括三部分内容：
- json to kafka
- kafka to redis
- redis to hbase

kafka to redis部分使用Kafka Streams API实现非法数据过滤。Json数据输入到json_input 主题，经过流API处理后，非法数据发布到redis wrong_channel，正确的数据写入json_output主题。

redis to hbase部分使用发布订阅。从right_channel和wrong_channel 两个频道读取数据写入HbaseRecord表和IllegalRecord表。

## 代码使用

ProducerMain, ConsumerMain, Stream中设置：
- kafkaIp: kafka Stream连接用的IP
- producerIp: 生产者IP
- consumerIp: 消费者IP
- inTopic: Stream输入主题
- outTopic: Stream输出主题
- rightChannel: 正确数据发布频道
- wrongChannel: 错误数据发布频道

RedisConf中设置：
- clusterIp: redis集群IP
- clusterPort: redis端口

HBaseConf中设置：
- "hbase.zookeeper.quorum": Hbase连接配置

## 代码运行
- 集群启动，创建inTopic, outTopic
- 运行main/Stream
- 运行main/ProducerMain
- 运行main/ConsumerMain
