package su.ptx.kfkadmin.kluster_client;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import su.ptx.GetFuture;

import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class RecordsTests extends KlusterTests {
  @Test
  void clearTopik(EmbeddedKafkaBroker broker) {
    int msgNum = 100;
    try (var producer = new KafkaProducer<Integer, String>(KafkaTestUtils.producerProps(broker))) {
      IntStream.range(0, msgNum)
        .mapToObj(number -> new ProducerRecord<>("foo", number, String.valueOf(number)))
        .map(producer::send)
        .forEach(rmdf -> new GetFuture<>().apply(rmdf));
    }
    var foo = kluster.topik("foo");
    assertEquals(msgNum, foo.size());
    var lowWatermarks = foo.clear();
    assertEquals(0, foo.size());
    assertEquals(3, lowWatermarks.length);
    assertEquals(msgNum, LongStream.of(lowWatermarks).sum());
  }
}
