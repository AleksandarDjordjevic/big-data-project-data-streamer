import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class KafkaProducer {
    Logger logger = LoggerFactory.getLogger(KafkaProducer.class);
    private Properties properties;
    private String streamingDelay = "data.stream.delay";
    private String streamingTopic = "data.stream.topic";

    public KafkaProducer(Properties properties) {
        this.properties = properties;
    }

    public void sendData(String fileName) {
        //Configure the Producer
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getProperty("kafka.bootstrap.address"));
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        //Read streaming data from file
        File file = new File(Producer.class.getResource("/" + fileName).getFile());

        //Produce line by line
        try (org.apache.kafka.clients.producer.Producer producer = new org.apache.kafka.clients.producer.KafkaProducer(configProperties)) {
            try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                String line;
                while ((line = br.readLine()) != null) {
                    Thread.sleep(Integer.parseInt(properties.getProperty(streamingDelay)));
                    producer.send(new ProducerRecord(properties.getProperty(streamingTopic), line));
                    System.out.println("Sending data:" + line);
                }
            } catch (InterruptedException | IOException e) {
                logger.info(e.getMessage(), e);
            }
        }
    }
}
