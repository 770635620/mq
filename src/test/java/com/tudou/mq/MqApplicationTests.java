package com.tudou.mq;


import com.tudou.mq.producer.KafkaProducer;
import com.tudou.mq.producer.RabbitProducer;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import javax.annotation.Resource;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaOperations.OperationsCallback;
import org.springframework.kafka.core.KafkaTemplate;

@SpringBootTest
class MqApplicationTests {

  @Resource
  RabbitProducer rabbitProducer;


  @Test
  public void directTest(){
    rabbitProducer.testDirectSender("testDirect");
  }

  @Test
  public void fanoutTest(){
    rabbitProducer.testFanoutSender("testFanout");
  }

  @Test
  public void delayTest() throws InterruptedException, IOException {
    rabbitProducer.testDelaySender("testDelay0");
    Thread.sleep(1000 * 2);
    rabbitProducer.testDelaySender("testDelay1");
    for (int i = 2; i < 30; i++) {
      System.out.println(i);
      Thread.sleep(1000);
    }
  }

  @Resource
  KafkaProducer kafkaProducer;

  @Resource
  AdminClient adminClient;

  @Resource
  KafkaTemplate kafkaTemplate;

  @Test
  public void kafkaTest() throws InterruptedException {
    /*kafkaTemplate.executeInTransaction(new OperationsCallback() {
      @Override
      public Object doInOperations(KafkaOperations kafkaOperations) {
        kafkaTemplate.send(new ProducerRecord("test-topic","kafka"));
        return null;
      }
    });*/
    kafkaProducer.tranSend("test-topic","lllllllllllllllllllllllllllllllll");

  }

  @Test
  public void getTopicInfo() throws ExecutionException, InterruptedException {
    adminClient.describeTopics(Arrays.asList("test-topic")).all().get().forEach((k,v) -> System.out.println("key: " + k + " v: " + v));
  }

  @Test
  public void testCreateTopic(){
    String [] options= new String[]{
        "--create",
        "--zookeeper","127.0.0.1:2181",
        "--replication-factor", "3",
        "--partitions", "3",
        "--topic", "topic-kl"
    };
    //TopicCommand.main(options);
  }
}
