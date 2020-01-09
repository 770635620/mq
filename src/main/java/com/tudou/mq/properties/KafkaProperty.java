package com.tudou.mq.properties;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "spring.kafka", ignoreUnknownFields = true)
public class KafkaProperty {

  /**
   * 服务列表
   */
  private String bootstrapServers;
  /**
   * Topic名称
   */
  private String topicName;
  /**
   * 分区的数量
   */
  private Integer partitionsNum;
  /**
   * 分区副本因子,2
   */
  private Short replicationFactor;

  private final Consumer consumer = new Consumer();
  private final Producer producer = new Producer();
  private final Listener listener = new Listener();

  @Setter
  @Getter
  public static class Consumer {

    /**
     * 默认消费者组
     */
    private String groupId;
    /**
     * 是否开启自动提交
     */
    private Boolean enableAutoCommit;
    /**
     * 自动提交间隔
     */
    private Integer autoCommitInterval;
    /**
     * 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费；当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
     */
    private String autoOffsetReset;
    /**
     * 批量消费一次最大拉取的数据量
     */
    private Integer maxPollRecords;
    /**
     * 连接超时时间
     */
    private Integer sessionTimeout;
    /**
     * 手动提交设置与poll的心跳数,如果消息队列中没有消息，等待毫秒后，调用poll()方法。如果队列中有消息，立即消费消息，每次消费的消息的多少可以通过max.poll.records配置。
     */
    private Integer maxPollInterval;
    /**
     * 设置拉取数据的大小,15M
     */
    private Integer maxPartitionFetchBytes;
    /**
     * Json反序列化信任包
     */
    private String trustedPackages;
  }

  @Setter
  @Getter
  public static class Producer {

    /**
     * 批处理缓冲区,32M
     */
    private int bufferMemory;
    /**
     * 发送失败后的重复发送次数
     */
    private Integer retries;
    /**
     * 一次最多发送数据量(batch.size和ling.ms之一，producer便开始发送消息)
     */
    private Integer batchSize;
    /**
     * 批处理延迟时间上限：即1ms过后，不管是否达到批处理数，都直接发送一次请求
     */
    private Integer lingerMs;
  }


  @Setter
  @Getter
  public static class Listener {
    /**
     * 是否开启批量消费，true表示批量消费
     */
    private Boolean batchListener;
    /**
     * 设置消费的线程数
     */
    private Integer concurrencys;
  }

}
