package com.baiyu.consumer;

import com.baiyu.common.KafkaProtitesContans;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

/**
 * @author baiyu
 * @description: KafkaConsumerThred
 * @date: 2018/7/29
 */
public class KafkaConsumerThred extends Thread {

    private final KafkaConsumer<String, String> kafkaConsumer;

    private final String topic;

    private final Properties props = new Properties();

    KafkaConsumerThred(String topic) {
        initProps();
        kafkaConsumer = new KafkaConsumer<String, String>(props);
        this.topic = topic;
    }

    void initProps(){
        props.put("bootstrap.servers", KafkaProtitesContans.KAFKA_SERVER_URL + ":" + KafkaProtitesContans.KAFKA_SERVER_PORT);
        props.put("group.id", KafkaProtitesContans.GROUP_ID);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", KafkaProtitesContans.RECONNECT_INTERVAL);
        props.put("session.timeout.ms", KafkaProtitesContans.CONNECTION_TIMEOUT);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    }


    @Override
    public void run() {
        kafkaConsumer.subscribe(Arrays.asList(topic));
        kafkaConsumer.seekToBeginning(new ArrayList<TopicPartition>());
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("fetched from partition " + record.partition() + ", offset: " + record.offset() + ", message: " + record.value());
            }
        }
    }
}
