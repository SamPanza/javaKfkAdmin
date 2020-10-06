package su.ptx.kfkadmin.kluster_client;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import su.ptx.GetFuture;

import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

@EmbeddedKafka(partitions = 3)
final class RecordsTests extends KlusterTests {
  @Test
  void clearTopik(EmbeddedKafkaBroker broker) {
    int N = 100;
    try (var producer = new KafkaProducer<Integer, String>(KafkaTestUtils.producerProps(broker))) {
      IntStream.range(0, N)
        .mapToObj(number -> new ProducerRecord<>("quuz", number, String.valueOf(number)))
        .map(producer::send)
        .forEach(rmdf -> new GetFuture<>().apply(rmdf));
    }
    var quuz = kluster.topik("quuz");
    assertEquals(N, quuz.size());
    var lowWatermarks = quuz.clear();
    assertEquals(0, quuz.size());
    assertEquals(3, lowWatermarks.length);
    assertEquals(N, LongStream.of(lowWatermarks).sum());
  }
}
