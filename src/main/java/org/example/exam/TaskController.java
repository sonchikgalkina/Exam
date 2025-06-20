package org.example.exam;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@RestController
@RequestMapping("/api/task")
public class TaskController {

    private final StringRedisTemplate redisTemplate;

    public TaskController(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @GetMapping("/processTopic")
    public String processTopic(
            @RequestParam String bootstrapServers,
            @RequestParam String topicName) throws InterruptedException {


        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "task-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Чтение с начала

        ConsumerFactory<String, String> consumerFactory =
                new DefaultKafkaConsumerFactory<>(consumerProps);

        CountDownLatch latch = new CountDownLatch(1);
        StringBuilder result = new StringBuilder();

        ContainerProperties containerProps = new ContainerProperties(topicName);
        KafkaMessageListenerContainer<String, String> container =
                new KafkaMessageListenerContainer<>(consumerFactory, containerProps);

        container.setupMessageListener((MessageListener<String, String>) record -> {
            String key = record.key();
            String redisValue = redisTemplate.opsForValue().get(key);

            if (redisValue != null) {
                for (int i = 0; i < redisValue.length(); i += 2) {
                    result.append(redisValue.charAt(i));
                }
            } else {
                result.append("Key '").append(key).append("' not found in Redis");
            }

            latch.countDown();
        });


        container.start();
        boolean messageConsumed = latch.await(10, TimeUnit.SECONDS); 
        container.stop();

        if (!messageConsumed) {
            return "No messages in topic or timeout exceeded";
        }

        return result.toString();
    }
}
