package com.baiyu.producer;

import com.baiyu.common.KafkaProtitesContans;
import org.apache.kafka.common.utils.KafkaThread;

/**
 * @author baiyu
 * @description: ProducerTest
 * @date: 2018/7/29
 */
public class ProducerTest {

    public static void main(String[] args) {
        ProducerThred producerThread=new ProducerThred(KafkaProtitesContans.TOPIC);
        producerThread.start();
    }
}
