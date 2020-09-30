package su.ptx.kfkadmin.kluster_client;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.test.context.EmbeddedKafka;
import su.ptx.kfkadmin.kluster.Kluster;
import su.ptx.kfkadmin.kluster.Kluster.Topik;
import su.ptx.kfkadmin.kluster.Kluster.Topik.Partition;

import static org.junit.jupiter.api.Assertions.*;

@EmbeddedKafka(count = 2, partitions = 3, topics = {"foo", "bar"})
class KlusterReadonlyTests {
  private static final Kluster KLUSTER = KlusterHolder.init();

  @AfterAll
  static void afterAll() {
    KlusterHolder.destroy();
  }

  @Test
  void testKlusterInfo() {
    var info = KLUSTER.info();
    assertEquals(2, info.nodes().size());
    assertTrue(info.nodes().contains(info.controller()));
  }

  @Test
  void testTopiks() {
    assertArrayEquals(
      new String[]{"bar", "foo"},
      KLUSTER.topiks().stream().map(Topik::name).toArray());
  }

  @Test
  void testPartitions() {
    assertArrayEquals(
      new int[]{0, 1, 2},
      KLUSTER.topik("foo").partitions().stream().mapToInt(Partition::id).toArray());
  }

  @Test
  void testTopikSize() {
    assertEquals(0, KLUSTER.topik("foo").size());
  }
}
