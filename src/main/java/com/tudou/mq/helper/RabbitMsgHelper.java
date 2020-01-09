package com.tudou.mq.helper;

import com.alibaba.fastjson.JSON;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;

/**
 * @author: Zhang Song
 * @since: 2019/12/26
 */
public class RabbitMsgHelper {

  public static Message objToMsg(Object obj,boolean delay,long delayTime) {
    if (null == obj) {
      return null;
    }

    Message message = MessageBuilder.withBody(JSON.toJSONString(obj).getBytes()).build();
    message.getMessageProperties().setContentType(MessageProperties.CONTENT_TYPE_JSON);
    message.getMessageProperties().setDeliveryMode(MessageDeliveryMode.PERSISTENT);
    if(delay){
      message.getMessageProperties().setHeader("x-delay",delayTime);
      message.getMessageProperties().setExpiration(String.valueOf(delayTime));
    }
    return message;
  }
}
