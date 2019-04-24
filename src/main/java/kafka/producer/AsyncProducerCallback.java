package kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class AsyncProducerCallback implements Callback {
  @Override
  public void onCompletion(RecordMetadata recordMetadata, Exception e) {
    if (e != null) {
      System.out.println("Error producing to topic " + recordMetadata.topic());
      e.printStackTrace();
    }
  }
}
