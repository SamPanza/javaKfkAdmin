package su.ptx.kfkadmin.kluster_client;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import su.ptx.kfkadmin.kluster.Kluster;

import java.util.Map;

@EmbeddedKafka(count = 2, partitions = 3, topics = {"foo", "bar"})
abstract class KlusterTests {
  Kluster kluster;

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
}
