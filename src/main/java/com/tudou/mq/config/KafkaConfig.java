package com.tudou.mq.config;

import com.tudou.mq.properties.KafkaProperty;
import com.tudou.mq.properties.KafkaProperty.Consumer;
import com.tudou.mq.properties.KafkaProperty.Producer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConsumerAwareListenerErrorHandler;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;

/**
 * @author: Zhang Song
 * @since: 2019/12/26
 */
@Configuration
@EnableKafka
@EnableConfigurationProperties(value = KafkaProperty.class)
@Slf4j
public class KafkaConfig {
  /**
   *
   * 测试 不同group有且仅有一个consumer会消费全部消息，newTopic后设置分区数每个consumer都会消费
   *     重置offset会重新消费消息
   * @param producerFactory
   * @return
   */

  @Resource
  private KafkaProperty kafkaProperty;

  //创建一个kafka管理类，相当于rabbitMQ的管理类rabbitAdmin,没有此bean无法自定义的使用adminClient创建topic
  @Bean
  public KafkaAdmin kafkaAdmin() {
    Map<String, Object> configs = new HashMap<>();
    configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
        kafkaProperty.getBootstrapServers());
    return new KafkaAdmin(configs);
  }

  //kafka客户端，在spring中创建这个bean之后可以注入并且创建topic
  @Bean
  public AdminClient adminClient() {
    return AdminClient.create(kafkaAdmin().getConfig());
  }

  //创建默认TopicName的Topic并设置分区数为以及副本数
  @Bean
  public NewTopic newTopic() {
    return new NewTopic(kafkaProperty.getTopicName(),
        kafkaProperty.getPartitionsNum(),
        kafkaProperty.getReplicationFactor());
  }

  //消费者配置参数
  private Map<String, Object> consumerProps() {
    Map<String, Object> props = new HashMap<>();
    Consumer consumer = kafkaProperty.getConsumer();

    //连接地址
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperty.getBootstrapServers());
    //GroupID
    props.put(ConsumerConfig.GROUP_ID_CONFIG, consumer.getGroupId());
    //是否自动提交 当使用ACK机制时 设置为false
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, consumer.getEnableAutoCommit());
    //自动提交的频率
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, consumer.getAutoCommitInterval());
    //Session超时设置
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, consumer.getSessionTimeout());
    //键的反序列化方式
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
    //值的反序列化方式
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    //一次拉取消息数量 批量监听时一次拉取消息条数
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "5");
    return props;
  }

  //生产者配置
  private Map<String, Object> senderProps() {
    Map<String, Object> props = new HashMap<>();
    Producer producer = kafkaProperty.getProducer();
    //连接地址
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperty.getBootstrapServers());
    //重试，0为不启用重试机制
    props.put(ProducerConfig.RETRIES_CONFIG, producer.getRetries());
    //控制批处理大小，单位为字节
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, producer.getBatchSize());
    //批量发送，延迟为1毫秒，启用该功能能有效减少生产者发送消息次数，从而提高并发量
    props.put(ProducerConfig.LINGER_MS_CONFIG, producer.getLingerMs());
    //生产者可以使用的总内存字节来缓冲等待发送到服务器的记录
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, producer.getBufferMemory());
    //键的序列化方式
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
    //值的序列化方式
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    return props;
  }

  /**
   * 开启事物---事务属性是2017年Kafka 0.11.0.0引入的新特性
   * @param
   * @return
   */
  @Bean
  public KafkaTransactionManager transactionManager(ProducerFactory producerFactory) {
    KafkaTransactionManager manager = new KafkaTransactionManager(producerFactory);
    return manager;
  }

  //ConcurrentKafkaListenerContainerFactory为创建Kafka监听器的工程类，这里只配置了消费者
  @Bean
  public ConcurrentKafkaListenerContainerFactory<Integer, String> kafkaListenerContainerFactory() {
    ConcurrentKafkaListenerContainerFactory<Integer, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerFactory());
    //禁止自动启动 可定时通过KafkaListenerEndpointRegistry 启动
    //factory.setAutoStartup(false);
    //最大重试三次
    /**
     * 监听器会尝试三次调用，当到达最大的重试次数后。消息就会被丢掉重试死信队列里面去。
     * 死信队列的Topic的规则是，业务Topic名字+“.DLT”
     */
    //factory.setErrorHandler(new SeekToCurrentErrorHandler(new DeadLetterPublishingRecoverer(kafkaTemplate), 3));
    return factory;
  }

  //批量监听
  @Bean("batchContainerFactory")
  public ConcurrentKafkaListenerContainerFactory batchContainerFactory() {

    ConcurrentKafkaListenerContainerFactory container = new ConcurrentKafkaListenerContainerFactory();
    container.setConsumerFactory(new DefaultKafkaConsumerFactory(consumerProps()));
    //设置并发量，小于或等于Topic的分区数
    container.setConcurrency(5);
    //设置为批量监听
    container.setBatchListener(true);
    return container;
  }

  //ACK监听
  @Bean("ackContainerFactory")
  public ConcurrentKafkaListenerContainerFactory ackContainerFactory() {
    ConcurrentKafkaListenerContainerFactory factory = new ConcurrentKafkaListenerContainerFactory();
    factory.getContainerProperties().setAckMode(AckMode.MANUAL_IMMEDIATE);
    factory.setConsumerFactory(new DefaultKafkaConsumerFactory(consumerProps()));
    return factory;
  }

  //转发监听 @sendTo 方式
  @Bean("rePlyContainerFactory")
  public ConcurrentKafkaListenerContainerFactory<Integer, String> rePlyContainerFactory() {
    ConcurrentKafkaListenerContainerFactory<Integer, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerFactory());
    factory.setReplyTemplate(kafkaTemplate());
    return factory;
  }

  //转发监听 ReplyingKafkaTemplate 方式

  /**
   * 配置ConcurrentKafkaListenerContainerFactory的ReplyTemplate
   * 配置topic.quick.request的监听器
   * 注册一个KafkaMessageListenerContainer类型的监听容器，监听topic.quick.reply，
   * 这个监听器里面我们不处理任何事情，交由ReplyingKafkaTemplate处理
   * 通过ProducerFactory和KafkaMessageListenerContainer创建一个ReplyingKafkaTemplate类型的Bean，
   * 设置回复超时时间为10秒
   * @return
   */
  @Bean
  public KafkaMessageListenerContainer<String, String> replyContainer(@Autowired ConsumerFactory consumerFactory) {
    ContainerProperties containerProperties = new ContainerProperties("topic.quick.reply");
    return new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);
  }
  @Bean
  public ReplyingKafkaTemplate<String, String, String> replyingKafkaTemplate(@Autowired ProducerFactory producerFactory, KafkaMessageListenerContainer replyContainer) {
    ReplyingKafkaTemplate template = new ReplyingKafkaTemplate<>(producerFactory, replyContainer);
    template.setReplyTimeout(10000);
    return template;
  }

  //消息过滤
  /**
   * 为监听容器工厂配置一个RecordFilterStrategy(消息过滤策略)，
   * 返回true的时候消息将会被抛弃，返回false时，消息能正常抵达监听容器
   * @return
   */
  @Bean("filterContainerFactory")
  public ConcurrentKafkaListenerContainerFactory filterContainerFactory() {
    ConcurrentKafkaListenerContainerFactory factory = new ConcurrentKafkaListenerContainerFactory();
    factory.setConsumerFactory(consumerFactory());
    //配合RecordFilterStrategy使用，被过滤的信息将被丢弃
    factory.setAckDiscarded(true);
    factory.setRecordFilterStrategy(consumerRecord -> {
      long data = Long.parseLong((String) consumerRecord.value());
      log.info("filterContainerFactory filter : "+data);
      if (data % 2 == 0) {
        return false;
      }
      //返回true将会被丢弃
      return true;
    });
    return factory;
  }




  //根据consumerProps填写的参数创建消费者工厂
  @Bean
  public ConsumerFactory<Integer, String> consumerFactory() {
    return new DefaultKafkaConsumerFactory<>(consumerProps());
  }

  //根据senderProps填写的参数创建生产者工厂
  @Bean
  public ProducerFactory<Integer, String> producerFactory() {
    DefaultKafkaProducerFactory factory = new DefaultKafkaProducerFactory<>(senderProps());
    // 开启事务
     factory.transactionCapable();
     factory.setTransactionIdPrefix("tran-");
    return factory;
  }

  //kafkaTemplate实现了Kafka发送接收等功能
  @Bean
  @Primary
  public KafkaTemplate<Integer, String> kafkaTemplate() {
    KafkaTemplate template = new KafkaTemplate<Integer, String>(producerFactory());
    template.setProducerListener(new ProducerListener() {
      @Override
      public void onSuccess(ProducerRecord producerRecord, RecordMetadata recordMetadata) {
        log.info("Message send success : " + producerRecord.toString());
      }

      @Override
      public void onError(ProducerRecord producerRecord, Exception exception) {
        log.info("Message send error : " + producerRecord.toString());
      }
    });
    return template;
  }

  @Bean("defaultKafkaTemplate")
  public KafkaTemplate<Integer, String> defaultKafkaTemplate() {
    KafkaTemplate template = new KafkaTemplate<Integer, String>(producerFactory());
    template.setDefaultTopic("topic.quick.default");
    return template;
  }


  //单异常处理
  @Bean
  public ConsumerAwareListenerErrorHandler consumerAwareErrorHandler() {
    return (message, e, consumer) -> {
      log.info("consumerAwareErrorHandler receive : "+message.getPayload().toString());
      return null;
    };
  }

  //批量消费异常处理器
  @Bean
  public ConsumerAwareListenerErrorHandler batchConsumerAwareErrorHandler() {
    return (message, e, consumer) -> {
      log.info("consumerAwareErrorHandler receive : "+message.getPayload().toString());
      MessageHeaders headers = message.getHeaders();
      List<String> topics = headers.get(KafkaHeaders.RECEIVED_TOPIC, List.class);
      List<Integer> partitions = headers.get(KafkaHeaders.RECEIVED_PARTITION_ID, List.class);
      List<Long> offsets = headers.get(KafkaHeaders.OFFSET, List.class);
      Map<TopicPartition, Long> offsetsToReset = new HashMap<>();
      return null;
    };
  }
}
