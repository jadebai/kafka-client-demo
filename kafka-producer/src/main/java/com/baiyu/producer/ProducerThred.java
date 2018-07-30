package com.baiyu.producer;

import com.baiyu.common.KafkaProtitesContans;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/**
 * @author baiyu
 * @description: ProducerThred
 * @date: 2018/7/29
 */
public class ProducerThred extends Thread{

    private final KafkaProducer<String,String> kafkaProducer;
    private final String topic;
    private final Properties props=new Properties();

    ProducerThred(String topic){
        initProps();
        kafkaProducer=new KafkaProducer<String,String>(props);
        this.topic=topic;
    }

    void initProps(){
        props.put("bootstrap.servers", KafkaProtitesContans.KAFKA_SERVER_URL+":"+ KafkaProtitesContans.KAFKA_SERVER_PORT);
        props.put("acks","all");
        props.put("retries",0);
        props.put("batch.size",KafkaProtitesContans.KAFKA_PRODUCER_BUFFER_SIZE);
        props.put("linger.ms",1);
        props.put("buffer.memory",33554432);
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    }

    @Override
    public void run() {
        int messageNo=1;
        while (true){
            String messageStr=new String("Message_"+messageNo);
            System.out.println("Send:"+messageStr);
            kafkaProducer.send(new ProducerRecord<String, String>(topic, messageStr), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e != null){
                        e.printStackTrace();
                    }
                    System.out.println("message send to partition " + recordMetadata.partition() + ", offset: " + recordMetadata.offset());
                }
            });
            messageNo++;
            try {
                sleep(1000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
}
