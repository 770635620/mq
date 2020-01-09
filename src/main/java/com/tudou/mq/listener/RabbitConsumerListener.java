package com.tudou.mq.listener;

import com.rabbitmq.client.Channel;
import com.tudou.mq.config.RabbitConfig;
import com.tudou.mq.consumer.BaseRabbitConsumer;
import com.tudou.mq.consumer.RabbitConsumer;
import com.tudou.mq.consumer.RabbitConsumer2;
import com.tudou.mq.helper.BaseRabbitConsumerProxy;
import java.io.IOException;
import javax.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

/**
 * @author: Zhang Song
 * @since: 2019/12/26
 */
@Component
@Slf4j
public class RabbitConsumerListener {

  @Resource
  private RabbitConsumer rabbitConsumer;

  @Resource
  private RabbitConsumer2 rabbitConsumer2;

  @RabbitListener(queues = RabbitConfig.TEST_FANOUT_QUEUE_NAME1)
  public void consumeA(Message message, Channel channel) throws IOException {
    BaseRabbitConsumerProxy baseConsumerProxy = new BaseRabbitConsumerProxy(rabbitConsumer);
    BaseRabbitConsumer proxy = (BaseRabbitConsumer) baseConsumerProxy.getProxy();
    if (null != proxy) {
      log.info("A");
      proxy.consume(message, channel);
    }
  }

  @RabbitListener(queues = RabbitConfig.TEST_FANOUT_QUEUE_NAME2)
  public void consumeB(Message message, Channel channel) throws IOException {
    BaseRabbitConsumerProxy baseConsumerProxy = new BaseRabbitConsumerProxy(rabbitConsumer2);
    BaseRabbitConsumer proxy = (BaseRabbitConsumer) baseConsumerProxy.getProxy();
    if (null != proxy) {
      log.info("B");
      proxy.consume(message, channel);
    }
  }

  @RabbitListener(queues = RabbitConfig.TEST_DELAY_QUEUE_NAME)
  public void consumeC(Message message, Channel channel) throws IOException {
    BaseRabbitConsumerProxy baseConsumerProxy = new BaseRabbitConsumerProxy(rabbitConsumer2);
    BaseRabbitConsumer proxy = (BaseRabbitConsumer) baseConsumerProxy.getProxy();
    if (null != proxy) {
      log.info("C");
      proxy.consume(message, channel);
    }
  }
}
