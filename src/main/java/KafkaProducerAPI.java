import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

class KafkaProducerAPI {

    // Kafka Server custom settings
    private String bootstrapServer = "";
    private String topic = "";

    private KafkaProducer<String, String> producer;

    KafkaProducerAPI(){
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.producer = new KafkaProducer<String, String>(properties);
    }

    void send(String msg){
        try{
            producer.send(new ProducerRecord<String, String>(this.topic, msg)).get();
        }catch(Exception e){
            e.printStackTrace();
        }
    }

}
