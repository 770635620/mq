package com.tudou.mq.producer;

import com.tudou.mq.config.RabbitConfig;
import com.tudou.mq.helper.RabbitMsgHelper;
import java.util.UUID;
import javax.annotation.Resource;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

/**
 * @author: Zhang Song
 * @since: 2019/12/26
 */

@Component
public class RabbitProducer {

  @Resource
  @Qualifier("masterRabbitTemplate")
  RabbitTemplate masterRabbitTemplate;

  public void testDirectSender(Object msg){
    CorrelationData correlationData = new CorrelationData(UUID.randomUUID().toString());
    masterRabbitTemplate.convertAndSend(RabbitConfig.TEST_DIRECT_EXCHANGE_NAME, RabbitConfig.TEST_ROUTING_KEY_NAME, RabbitMsgHelper
        .objToMsg(msg,false,0L), correlationData);
  }

  public void testFanoutSender(Object msg){
    CorrelationData correlationData = new CorrelationData(UUID.randomUUID().toString());
    masterRabbitTemplate.convertAndSend(RabbitConfig.TEST_FANOUT_EXCHANGE_NAME,RabbitConfig.TEST_ROUTING_KEY_NAME, RabbitMsgHelper
        .objToMsg(msg,false,0L), correlationData);
  }

  public void testDelaySender(Object msg){
    CorrelationData correlationData = new CorrelationData(UUID.randomUUID().toString());
    masterRabbitTemplate.convertAndSend(RabbitConfig.TEST_DELAY_PRE_EXCHANGE_NAME,RabbitConfig.TEST_DELAY_PRE_QUEUE_NAME, RabbitMsgHelper
        .objToMsg(msg,true,1000 * 15), correlationData);
  }
}
