package com.tudou.mq.producer;

import java.util.concurrent.ExecutionException;
import javax.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaOperations.OperationsCallback;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

/**
 * @author: Zhang Song
 * @since: 2020/1/8
 *
 * topic：这里填写的是Topic的名字
 * partition：这里填写的是分区的id，其实也是就第几个分区，id从0开始。表示指定发送到该分区中
 * timestamp：时间戳，一般默认当前时间戳
 * key：消息的键
 * data：消息的数据
 * ProducerRecord：消息对应的封装类，包含上述字段
 * Message<?>：Spring自带的Message封装类，包含消息及消息头
 *
 */
@Component
@Slf4j
public class KafkaProducer{

  @Resource
  KafkaTemplate<String,Object> kafkaTemplate;

  public void send(String topic,Object msg){
    ListenableFuture<SendResult<String,Object>> future = kafkaTemplate.send(topic, msg);
    //可具体到方法 回调
    future.addCallback(new ListenableFutureCallback<SendResult<String, Object>>() {
      @Override
      public void onFailure(Throwable throwable) {
        log.error("KafkaProduce report msg {} , failure, cause : {}", msg, throwable);
      }

      @Override
      public void onSuccess(SendResult<String, Object> sendResult) {
        log.info("KafkaProduce report msg {} , success", msg);
      }
    });
  }

  //开启事务
  public void tranSend(String topic,Object msg){
    kafkaTemplate.executeInTransaction((OperationsCallback) kafkaOperations -> {
      kafkaOperations.send(topic,msg);
      //throw new RuntimeException("sssssssss");
      return null;
    });
  }

  //同步发送

  /**
   * get(long timeout, TimeUnit unit)，当send方法耗时大于get方法所设定的参数时会抛出一个超时异常，
   * 但需要注意，这里仅抛出异常，消息还是会发送成功的。
   * 运行后我们可以看到抛出的异常，但也发现消息能发送成功并被监听器接收了。
   * 使用该方法作为SQL慢查询记录的条件
   *
   * @param topic
   * @param msg
   * @throws ExecutionException
   * @throws InterruptedException
   */
  public void syncSend(String topic,Object msg) throws ExecutionException, InterruptedException {
    kafkaTemplate.send(topic, msg).get();
  }

}
