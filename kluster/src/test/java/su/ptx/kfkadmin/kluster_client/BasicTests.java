package su.ptx.kfkadmin.kluster_client;

import org.junit.jupiter.api.Test;
import su.ptx.kfkadmin.kluster.Kluster.Topik;
import su.ptx.kfkadmin.kluster.Kluster.Topik.Partition;

import static org.junit.jupiter.api.Assertions.*;

final class BasicTests extends KlusterTests {
  @Test
  void info_ok() {
    var info = kluster.info();
    assertEquals(2, info.nodes().size());
    assertTrue(info.nodes().contains(info.controller()));
  }

  @Test
  void topics_here_and_sorted(Topik t2, Topik t1) {
    assertNotNull(t2);
    assertNotNull(t1);
    assertArrayEquals(
      new String[]{"t1", "t2"},
      kluster.topiks().stream().map(Topik::name).toArray());
  }

  @Test
  void partitions_here_and_sorted(Topik t3) {
    assertArrayEquals(
      new int[]{0, 1, 2},
      t3.partitions().stream().mapToInt(Partition::id).toArray());
  }

  @Test
  void new_topic_is_empty(Topik t4) {
    assertEquals(0, t4.size());
  }
}
