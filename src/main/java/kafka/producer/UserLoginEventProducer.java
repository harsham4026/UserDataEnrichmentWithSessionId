package kafka.producer;

import java.sql.Timestamp;
import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.*;

public class UserLoginEventProducer {

  private static String topicName;
  private static int waitCycle; // in seconds
  private static int maxCount;

  private static void produceAsync() {

    String key = "Key1";

    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

    Producer<String, String> producer = new KafkaProducer<>(props);

    /**
     * Added shutdown hook for flushing the data that is yet to be pushed when the producer process is died.
     * Shuts down the producer gracefully.
     **/
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        producer.flush();
        producer.close(10000, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        System.out.println("failed to close the producer");
        e.printStackTrace();
      }
    }));

    while (true) {
      try {
        int countPerSec = new Random().nextInt(maxCount);
        for (int i = 0; i < countPerSec; i++) {
          Double lat1 = 40.68786621 + Double.valueOf(Math.random() * (40.74279785 - 40.68786621));
          Double long1 = -74.1796875 + Double.valueOf(Math.random() * (-74.15771484 + 74.1796875));
          Timestamp ts = new Timestamp(new Date().getTime());
          String value = lat1 + "," + long1 + "," + ts.toString();
          ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);

          /**
           *  Produce a record without waiting for server. This includes a callback that will print an error if something goes wrong.
           Basically producing and sending records asynchronously.
           **/

          producer.send(record, new AsyncProducerCallback());
        }

        Thread.sleep(waitCycle);

      } catch (Exception ex) {
        ex.printStackTrace(System.out);
        producer.close();
      }
    }
  }

  public static void main(String[] args) {
    produceAsync();
  }
}