package kafka.producer;

import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.*;

public class UserLoginEventProducer {

  private static String topicName;
  private static int waitCycle; // in seconds
  private static int maxCount;
  private static List<String> userIds;

  private static void generateUserIds() {
    userIds = new ArrayList<>();

    for (int i = 1; i <= 100; i++) {
      userIds.add("userId" + Integer.toString(i));
    }
  }

  private static void pushToTopic(Producer producer) {
    Random random = new Random();
    int userIdsSize = userIds.size();
    while (true) {
      try {
        int countPerSec = new Random().nextInt(maxCount);
        for (int i = 0; i < countPerSec; i++) {
          Timestamp userLoginTimeStamp = new Timestamp(new Date().getTime());
          String key = userIds.get(random.nextInt(userIdsSize));
          ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, userLoginTimeStamp.toString());

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

  private static void produceAsync() {
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
    generateUserIds();

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        producer.flush();
        producer.close(10000, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        System.out.println("failed to close the producer");
        e.printStackTrace();
      }
    }));

    pushToTopic(producer);
  }

  public static void main(String[] args) {
    produceAsync();
  }
}