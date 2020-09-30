package su.ptx.kfkadmin.kluster_client;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import su.ptx.kfkadmin.kluster.Kluster;
import su.ptx.kfkadmin.kluster.Kluster.Topik;
import su.ptx.kfkadmin.kluster.Kluster.Topik.Partition;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@EmbeddedKafka(count = 2, partitions = 3, topics = {"foo", "bar"})
class KlusterTests {
  private Kluster kluster;

  @BeforeEach
  void setUp() {
    kluster = new Kluster(
      Admin.create(
        Map.of(
          CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
          System.getProperty(EmbeddedKafkaBroker.SPRING_EMBEDDED_KAFKA_BROKERS))));
  }

  @AfterEach
  void tearDown() {
    kluster.close();
  }

  @Test
  void testKlusterInfo() {
    var info = kluster.info();
    var controller = info.controller();
    var nodes = info.nodes();
    assertTrue(nodes.contains(controller));
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
