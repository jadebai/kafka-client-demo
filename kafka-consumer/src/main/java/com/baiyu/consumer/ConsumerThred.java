package com.baiyu.consumer;

import com.baiyu.common.KafkaProtitesContans;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author baiyu
 * @description: ConsumerThred
 * @date: 2018/7/29
 */
public class ConsumerThred extends Thread {

    private final ConsumerConnector consumer;
    private final String topic;
    private final Properties props = new Properties();

    ConsumerThred(String topic){
        props.put("zookeeper.connect", KafkaProtitesContans.ZKCONNECT);
        props.put("group.id",KafkaProtitesContans.GROUP_ID);
        props.put("zookeeper.session.timeout.ms",KafkaProtitesContans.CONNECTION_TIMEOUT);
        props.put("zookeeper.sync.time.ms","200");
        props.put("auto.commit.interval.ms",KafkaProtitesContans.RECONNECT_INTERVAL);
        consumer=kafka.consumer.Consumer.createJavaConsumerConnector(
                new ConsumerConfig(props));
        this.topic = topic;
    }

    @Override
    public void run() {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(1));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()) {
            System.out.println("receive：" + new String(it.next().message()));
            try {
                sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
