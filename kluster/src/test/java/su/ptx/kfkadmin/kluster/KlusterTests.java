package su.ptx.kfkadmin.kluster;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.util.Map;

import static java.lang.System.err;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    var id = info.id();
    var controller = info.controller();
    var nodes = info.nodes();
    err.println(id);
    err.println(controller);
    err.println(nodes);
    assertTrue(nodes.contains(controller));
  }
}
