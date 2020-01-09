package com.tudou.mq.consumer;

import com.rabbitmq.client.Channel;
import java.io.IOException;
import org.springframework.amqp.core.Message;

/**
 * @author: Zhang Song
 * @since: 2019/12/26
 */
public interface BaseRabbitConsumer {
  void consume(Message message, Channel channel) throws IOException;
}
