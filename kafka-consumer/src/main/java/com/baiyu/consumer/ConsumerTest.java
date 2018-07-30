package com.baiyu.consumer;

import com.baiyu.common.KafkaProtitesContans;

import java.util.Properties;

/**
 * @author baiyu
 * @description: com.baiyu.consumer.ConsumerTest
 * @date: 2018/7/29
 */
public class ConsumerTest {

    public static void main(String[] args) {
//        ConsumerThred consumerThred = new ConsumerThred(KafkaProtitesContans.TOPIC);
//        consumerThred.start();
        KafkaConsumerThred kafkaConsumerThred = new KafkaConsumerThred(KafkaProtitesContans.TOPIC);
        kafkaConsumerThred.start();
    }
}
