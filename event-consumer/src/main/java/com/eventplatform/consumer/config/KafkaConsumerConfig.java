package com.eventplatform.consumer.config;

import com.eventplatform.common.model.Alert;
import com.eventplatform.common.model.EventAggregate;
import com.eventplatform.common.serialization.JsonSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    private Map<String, Object> baseProps(String groupId) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,  bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,            groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,   "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,  false);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,    100);
        return props;
    }

    // ── EventAggregate consumer ───────────────────────────────────────────

    @Bean
    public ConsumerFactory<String, EventAggregate> aggregateConsumerFactory() {
        JsonDeserializer<EventAggregate> deser = new JsonDeserializer<>(EventAggregate.class);
        deser.setRemoveTypeHeaders(false);
        deser.addTrustedPackages("com.eventplatform.*");
        deser.setUseTypeMapperForKey(false);
        return new DefaultKafkaConsumerFactory<>(
            baseProps("event-consumer-group"),
            new StringDeserializer(),
            deser
        );
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, EventAggregate>
            aggregateListenerContainerFactory() {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, EventAggregate>();
        factory.setConsumerFactory(aggregateConsumerFactory());
        factory.setConcurrency(3);
        return factory;
    }

    // ── Alert consumer ────────────────────────────────────────────────────

    @Bean
    public ConsumerFactory<String, Alert> alertConsumerFactory() {
        JsonDeserializer<Alert> deser = new JsonDeserializer<>(Alert.class);
        deser.setRemoveTypeHeaders(false);
        deser.addTrustedPackages("com.eventplatform.*");
        deser.setUseTypeMapperForKey(false);
        return new DefaultKafkaConsumerFactory<>(
            baseProps("alert-consumer-group"),
            new StringDeserializer(),
            deser
        );
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Alert>
            alertListenerContainerFactory() {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, Alert>();
        factory.setConsumerFactory(alertConsumerFactory());
        factory.setConcurrency(1);
        return factory;
    }
}
