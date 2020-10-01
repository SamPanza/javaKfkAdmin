package su.ptx.kfkadmin.kluster_client;

import org.junit.jupiter.api.Test;
import org.springframework.kafka.test.EmbeddedKafkaBroker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

final class RecordsTests extends KlusterTests {
  @Test
  void clearTopik(EmbeddedKafkaBroker broker) {
    assertNotNull(broker);
    var foo = kluster.topik("foo");
    //TODO produce some records before clear()
    var lowWatermarks = foo.clear();
    assertEquals(3, lowWatermarks.length);
  }
}
