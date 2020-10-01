package su.ptx.kfkadmin.kluster_client;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class CreateTopikTests extends KlusterTests {
  @Test
  void createWithDefaults() {
    var baz = kluster.createTopik("baz");
    assertEquals(3, baz.partitions().size());
    baz.delete();
  }

  @Test
  void createWithNumPartitionsSpecified() {
    var qux = kluster.createTopik("qux", 1);
    assertEquals(1, qux.partitions().size());
    qux.delete();
  }

  @Test
  void createWithNumPartitionsAndReplicationFactorSpecified() {
    var quux = kluster.createTopik("quux", 1, (short) 2);
    assertEquals(2, quux.partitions().first().replicas().size());
    quux.delete();
  }
}
