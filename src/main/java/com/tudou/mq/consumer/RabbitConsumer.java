package com.tudou.mq.consumer;

import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.stereotype.Component;

/**
 * @author: Zhang Song
 * @since: 2019/12/26
 */
@Component
@Slf4j
public class RabbitConsumer implements BaseRabbitConsumer{

  @Override
  public void consume(Message message, Channel channel){
    log.info("收到消息a: {}",message.toString());
  }
}
