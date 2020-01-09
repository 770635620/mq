package com.tudou.mq.listener;

import java.util.List;
import javax.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;

/**
 *
 * @author: Zhang Song
 * @since: 2020/1/8
 */

@Component
@Slf4j
public class KafkaConsumerListener {

  /**
   * id：消费者的id，当GroupId没有被配置的时候，默认id为GroupId
   * containerFactory：上面提到了@KafkaListener区分单数据还是多数据消费只需要配置一下注解的containerFactory属性就可以了，这里面配置的是监听容器工厂，也就是ConcurrentKafkaListenerContainerFactory，配置BeanName
   * topics：需要监听的Topic，可监听多个
   * topicPartitions：可配置更加详细的监听信息，必须监听某个Topic中的指定分区，或者从offset为200的偏移量开始监听
   * errorHandler：监听异常处理器，配置BeanName
   * groupId：消费组ID
   * idIsGroup：id是否为GroupId
   * clientIdPrefix：消费者Id前缀
   * beanRef：真实监听容器的BeanName，需要在 BeanName前加 "__"
   * @param record
   */
  @KafkaListener(topics = "${spring.kafka.topic-name}",groupId = "${spring.kafka.consumer.group-id}")
  public void consume(ConsumerRecord<?, ?> record){
    log.info("receive msg：{}",record.toString());
  }

  //批量监听
  @KafkaListener(id = "batch",clientIdPrefix = "batch",topics = {"topic.quick.batch"},containerFactory = "batchContainerFactory")
  public void batchListener(List<String> data) {
    log.info("topic.quick.batch  receive : ");
    for (String s : data) {
      log.info(  s);
    }
  }

  /**
   * 监听Topic中指定的分区
   *
   * 使用@KafkaListener注解的topicPartitions属性监听不同的partition分区。
   * @TopicPartition： topic--需要监听的Topic的名称，partitions --需要监听Topic的分区id，
   * partitionOffsets --可以设置从某个偏移量开始监听
   * @PartitionOffset： partition --分区Id，非数组，initialOffset --初始偏移量
   *
   * @param data
   */
  @KafkaListener(id = "batchWithPartition",clientIdPrefix = "bwp",containerFactory = "batchContainerFactory",
      topicPartitions = {
          @TopicPartition(topic = "topic.quick.batch.partition",partitions = {"1","3"}),
          @TopicPartition(topic = "topic.quick.batch.partition",partitions = {"0","4"},
              partitionOffsets = @PartitionOffset(partition = "2",initialOffset = "100"))
      }
  )
  public void batchListenerWithPartition(List<String> data) {
    log.info("topic.quick.batch.partition  receive : ");
    for (String s : data) {
      log.info(s);
    }
  }


  /**
   * 注解方式获取消息头及消息体
   *
   * 当你接收的消息包含请求头，以及你监听方法需要获取该消息非常多的字段时可以通过这种方式，
   * 这里使用的是默认的监听容器工厂创建的，如果你想使用批量消费，把对应的类型改为List即可，
   * 比如List<String> data ， List<Integer> key。
   *
   * @Payload： 获取的是消息的消息体，也就是发送内容
   * @Header (KafkaHeaders.RECEIVED_MESSAGE_KEY)：获取发送消息的key
   * @Header (KafkaHeaders.RECEIVED_PARTITION_ID)：获取当前消息是从哪个分区中监听到的
   * @Header (KafkaHeaders.RECEIVED_TOPIC)：获取监听的TopicName
   * @Header (KafkaHeaders.RECEIVED_TIMESTAMP)：获取时间戳
   *
   * @param data
   * @param key
   * @param partition
   * @param topic
   * @param ts
   */
  @KafkaListener(id = "anno", topics = "topic.quick.anno")
  public void annoListener(@Payload String data,
      @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) Integer key,
      @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
      @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
      @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long ts) {
    log.info("topic.quick.anno receive : \n"+
        "data : "+data+"\n"+
        "key : "+key+"\n"+
        "partitionId : "+partition+"\n"+
        "topic : "+topic+"\n"+
        "timestamp : "+ts+"\n"
    );
  }

  //ACK手动确认
  @KafkaListener(id = "ack", topics = "topic.quick.ack",containerFactory = "ackContainerFactory")
  public void ackListener(ConsumerRecord record, Acknowledgment ack) {
    log.info("topic.quick.ack receive : " + record.value());
    ack.acknowledge();
  }

  @Resource
  KafkaTemplate kafkaTemplate;

  /**
   * 重新将消息发送到队列中，这种方式比较简单而且可以使用Headers实现第几次消费的功能，用以下次判断
   * @param record
   * @param ack
   * @param consumer
   */
  @KafkaListener(id = "ack", topics = "topic.quick.ack", containerFactory = "ackContainerFactory")
  public void ackListener(ConsumerRecord record, Acknowledgment ack, Consumer consumer) {
    log.info("topic.quick.ack receive : " + record.value());

    //如果偏移量为偶数则确认消费，否则拒绝消费
    if (record.offset() % 2 == 0) {
      log.info(record.offset()+"--ack");
      ack.acknowledge();
    } else {
      log.info(record.offset()+"--nack");
      kafkaTemplate.send("topic.quick.ack", record.value());
    }
  }

  /**
   * 使用Consumer.seek方法，重新回到该未ack消息偏移量的位置重新消费，这种可能会导致死循环，
   * 原因出现于业务一直没办法处理这条数据，但还是不停的重新定位到该数据的偏移量上
   * @param record
   * @param ack
   * @param consumer
   */
  @KafkaListener(id = "ack", topics = "topic.quick.ack", containerFactory = "ackContainerFactory")
  public void ackListener1(ConsumerRecord record, Acknowledgment ack, Consumer consumer) {
    log.info("topic.quick.ack receive : " + record.value());

    //如果偏移量为偶数则确认消费，否则拒绝消费
    if (record.offset() % 2 == 0) {
      log.info(record.offset()+"--ack");
      ack.acknowledge();
    } else {
      log.info(record.offset()+"--nack");
      consumer.seek(new org.apache.kafka.common.TopicPartition("topic.quick.ack",record.partition()),record.offset() );
    }
  }


  /**
   * @SendTo 是直接将监听方法的返回值转发对应的Topic中，
   * 而ReplyTemplate也是将监听方法的返回值转发Topic中，但转发Topic成功后，会被请求者消费。
   * @param data
   * @return
   */
  @KafkaListener(id = "forward", topics = "topic.quick.target")
  @SendTo("topic.quick.real")
  public String forward(String data) {
    log.info("topic.quick.target  forward "+data+" to  topic.quick.real");
    return "topic.quick.target send msg : " + data;
  }

}
