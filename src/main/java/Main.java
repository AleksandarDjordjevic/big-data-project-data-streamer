import java.util.Properties;

public class Main {

    public static void main(String[] argv) throws Exception {
        Properties props = new Properties();
        props.load(Main.class.getResourceAsStream("application.properties"));
        KafkaProducer kafkaProducer = new KafkaProducer(props);
        kafkaProducer.sendData("data.txt");
    }
}
