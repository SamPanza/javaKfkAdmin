package su.ptx.kfkadmin.kluster_client;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class RecordsTests extends KlusterTests {
  @Test
  void clearTopik() {
    var N = 100;
    IntStream.range(0, N)
      .mapToObj(i -> new ProducerRecord<>("quuz", i, String.valueOf(i)))
      .forEach(intStrProducer::send);
    intStrProducer.flush();
    var quuz = kluster.topik("quuz");
    assertEquals(N, quuz.size());
    var lowWatermarks = quuz.clear();
    assertEquals(0, quuz.size());
    assertEquals(3, lowWatermarks.length);
    assertEquals(N, LongStream.of(lowWatermarks).sum());
    quuz.delete();
  }
}
