package com.tudou.mq.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @author: Zhang Song
 * @since: 2019/12/26
 */
@Data
@Configuration
@ConfigurationProperties(prefix = "spring.rabbitmq.cluster")
public class RabbitClusterProperty {

}
