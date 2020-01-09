package com.tudou.mq.helper;

import com.rabbitmq.client.Channel;
import java.lang.reflect.Proxy;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

/**
 * @author: Zhang Song
 * @since: 2019/12/26
 */

@Slf4j
public class BaseRabbitConsumerProxy {

  private Object target;

  public BaseRabbitConsumerProxy(Object target) {
    this.target = target;
  }

  public Object getProxy() {
    ClassLoader classLoader = target.getClass().getClassLoader();
    Class[] interfaces = target.getClass().getInterfaces();

    Object proxy = Proxy.newProxyInstance(classLoader, interfaces, (proxy1, method, args) -> {
      Message message = (Message) args[0];
      Channel channel = (Channel) args[1];

      String correlationId = getCorrelationId(message);

      if (isConsumed(correlationId)) {// 消费幂等性, 防止消息被重复消费
        log.info("重复消费, correlationId: {}", correlationId);
        return null;
      }

      MessageProperties properties = message.getMessageProperties();
      long tag = properties.getDeliveryTag();

      try {
        Object result = method.invoke(target, args);// 真正消费的业务逻辑
        //更新数据库状态
        channel.basicAck(tag, false);// 消费确认
        return result;
      } catch (Exception e) {
        log.error("getProxy error", e);
        channel.basicNack(tag, false, true);
        return null;
      }
    });

    return proxy;
  }

  /**
   * 获取CorrelationId
   *
   * @param message
   * @return
   */
  private String getCorrelationId(Message message) {
    String correlationId = null;

    MessageProperties properties = message.getMessageProperties();
    Map<String, Object> headers = properties.getHeaders();
    for (Map.Entry entry : headers.entrySet()) {
      String key = (String) entry.getKey();
      Object value = entry.getValue();
      if (key.equals("spring_returned_message_correlation")) {
        correlationId = value.toString();
      }
    }

    return correlationId;
  }

  /**
   * 消息是否已被消费
   *
   * @param correlationId
   * @return
   */
  private boolean isConsumed(String correlationId) {
    //查询数据库状态 判断是否已消费
    return false;
  }
}
