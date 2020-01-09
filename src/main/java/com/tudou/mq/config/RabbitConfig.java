package com.tudou.mq.config;

import com.rabbitmq.client.AMQP;
import com.tudou.mq.properties.RabbitClusterProperty;
import com.tudou.mq.properties.RabbitMasterProperty;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.CustomExchange;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory.ConfirmType;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

/**
 * @author: Zhang Song
 * @since: 2019/12/26
 */
@Configuration
@Slf4j
public class RabbitConfig {

  @Resource
  private RabbitMasterProperty masterProperty;

  @Resource
  private RabbitClusterProperty clusterProperty;

  private CachingConnectionFactory initConnectionFactory(String host, int port, String username,
      String password, String virtualHost, String connectName){
    CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
    connectionFactory.setConnectionNameStrategy((c) -> connectName);
    connectionFactory.setHost(host);
    connectionFactory.setPort(port);
    connectionFactory.setUsername(username);
    connectionFactory.setPassword(password);
    connectionFactory.setVirtualHost(virtualHost);
    connectionFactory.setPublisherConfirmType(ConfirmType.CORRELATED);
    connectionFactory.setPublisherReturns(true);
    return connectionFactory;
  }

  @Bean
  public Jackson2JsonMessageConverter converter() {
    return new Jackson2JsonMessageConverter();
  }

  @Bean(name = "masterConnectionFactory")
  @Primary
  public CachingConnectionFactory masterConnectionFactory(){
    return initConnectionFactory(masterProperty.getHost(),masterProperty.getPort(),masterProperty.getUsername(),masterProperty.getPassword(),masterProperty.getVirtualHost(),"master");
  }

  @Bean(name = "masterRabbitTemplate")
  public RabbitTemplate rabbitTemplate() {
    RabbitTemplate rabbitTemplate = new RabbitTemplate(masterConnectionFactory());
    rabbitTemplate.setMessageConverter(converter());

    // 消息是否成功发送到Exchange
    rabbitTemplate.setConfirmCallback((correlationData, ack, cause) -> {
      if (ack) {
        String msgId = correlationData.getId();
        log.info("消息成功发送到Exchange,msgId: {}",msgId);
        //持久化到数据库
      } else {
        log.info("消息发送到Exchange失败, {}, cause: {}", correlationData, cause);
      }
    });
    // 触发setReturnCallback回调必须设置mandatory=true, 否则Exchange没有找到Queue就会丢弃掉消息, 而不会触发回调
    rabbitTemplate.setMandatory(true);
    // 消息是否从Exchange路由到Queue, 注意: 这是一个失败回调, 只有消息从Exchange路由到Queue失败才会回调这个方法
    rabbitTemplate.setReturnCallback((message, replyCode, replyText, exchange, routingKey) -> {
      log.info("消息从Exchange路由到Queue失败: exchange: {}, route: {}, replyCode: {}, replyText: {}, message: {}", exchange, routingKey, replyCode, replyText, message);
    });

    return rabbitTemplate;
  }


  public static final String TEST_DIRECT_QUEUE_NAME = "test.direct.queue";
  public static final String TEST_DIRECT_EXCHANGE_NAME = "test.direct.exchange";
  public static final String TEST_ROUTING_KEY_NAME = "test.direct.routing.key";

  @Bean
  public Queue directQueue() {
    return new Queue(TEST_DIRECT_QUEUE_NAME, true);
  }
  @Bean
  public DirectExchange directExchange() {
    return new DirectExchange(TEST_DIRECT_EXCHANGE_NAME, true, false);
  }
  @Bean
  public Binding directBinding() {
    return BindingBuilder.bind(directQueue()).to(directExchange()).with(TEST_ROUTING_KEY_NAME);
  }

  public static final String TEST_FANOUT_QUEUE_NAME1 = "test.fanout.queue1";
  public static final String TEST_FANOUT_QUEUE_NAME2 = "test.fanout.queue2";
  public static final String TEST_FANOUT_EXCHANGE_NAME = "test.fanout.exchange";

  @Bean
  public Queue fanoutQueue1() {
    return new Queue(TEST_FANOUT_QUEUE_NAME1, true);
  }

  @Bean
  public Queue fanoutQueue2() {
    return new Queue(TEST_FANOUT_QUEUE_NAME2, true);
  }

  @Bean
  public FanoutExchange fanoutExchange() {
    return new FanoutExchange(TEST_FANOUT_EXCHANGE_NAME, true, false);
  }

  @Bean
  public Binding fanoutABinding() {
    return BindingBuilder.bind(fanoutQueue1()).to(fanoutExchange());
  }

  @Bean
  public Binding fanoutBBinding() {
    return BindingBuilder.bind(fanoutQueue2()).to(fanoutExchange());
  }


  public static final String TEST_DELAY_QUEUE_NAME = "test.delay.queue";
  public static final String TEST_DELAY_PRE_QUEUE_NAME = "test.delay.pre.queue";
  public static final String TEST_DELAY_EXCHANGE_NAME = "test.delay.exchange";
  public static final String TEST_DELAY_PRE_EXCHANGE_NAME = "test.delay.pre.exchange";

  @Bean
  public DirectExchange delayExchange(){
    return new DirectExchange(TEST_DELAY_EXCHANGE_NAME);
  }

  @Bean
  public DirectExchange preDelayExchange(){
    return new DirectExchange(TEST_DELAY_PRE_EXCHANGE_NAME);
  }

  @Bean
  public Queue delayQueue(){
    return new Queue(TEST_DELAY_QUEUE_NAME,true);
  }

  @Bean
  public Queue preDelayQueue(){
    Map<String, Object> args = new HashMap<>();
    args.put("x-dead-letter-exchange",TEST_DELAY_EXCHANGE_NAME);
    args.put("x-dead-letter-routing-key",TEST_DELAY_QUEUE_NAME);
    //args.put("x-message-ttl",1000 * 20);
    return new Queue(TEST_DELAY_PRE_QUEUE_NAME,true,false,false,args);
  }

  @Bean
  public Binding delayBinding() {
    return BindingBuilder.bind(delayQueue()).to(delayExchange()).with(TEST_DELAY_QUEUE_NAME);
  }

  @Bean
  public Binding preBinding() {
    return BindingBuilder.bind(preDelayQueue()).to(preDelayExchange()).with(TEST_DELAY_PRE_QUEUE_NAME);
  }

 /* @Bean
  public CustomExchange delayExchange() {
    Map<String, Object> args = new HashMap<>();
    args.put("x-delayed-type", "direct");
    return new CustomExchange(TEST_DELAY_EXCHANGE_NAME, "x-delayed-message",true, false,args);
  }
  @Bean
  public Queue delayQueue() {
    return new Queue(TEST_DELAY_QUEUE_NAME, true);
  }

  @Bean
  public Binding binding() {
    return BindingBuilder.bind(delayQueue()).to(delayExchange()).with(TEST_ROUTING_KEY_NAME).noargs();
  }*/


}
