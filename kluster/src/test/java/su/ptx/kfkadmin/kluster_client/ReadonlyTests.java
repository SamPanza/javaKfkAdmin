package su.ptx.kfkadmin.kluster_client;

import org.junit.jupiter.api.Test;
import su.ptx.kfkadmin.kluster.Kluster.Topik;
import su.ptx.kfkadmin.kluster.Kluster.Topik.Partition;

import static org.junit.jupiter.api.Assertions.*;

final class ReadonlyTests extends KlusterTests {
  @Test
  void testKlusterInfo() {
    var info = kluster.info();
    assertEquals(2, info.nodes().size());
    assertTrue(info.nodes().contains(info.controller()));
  }

  @Test
  void testTopiks() {
    assertArrayEquals(
      new String[]{"bar", "foo"},
      kluster.topiks().stream().map(Topik::name).toArray());
  }

  @Test
  void testPartitions() {
    assertArrayEquals(
      new int[]{0, 1, 2},
      kluster.topik("foo").partitions().stream().mapToInt(Partition::id).toArray());
  }

  @Test
  void testTopikSize() {
    assertEquals(0, kluster.topik("foo").size());
  }
}
