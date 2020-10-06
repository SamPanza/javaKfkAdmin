package su.ptx.kfkadmin.kluster_client;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import su.ptx.kfkadmin.kluster.Kluster;

import java.util.Map;

@EmbeddedKafka(count = 2, partitions = 3)
abstract class KlusterTests {
  Kluster kluster;
  @RegisterExtension
  ResolveTopikParameters resolveTopikParameters = new ResolveTopikParameters();
  Producer<Integer, String> intStrProducer;

  @BeforeEach
  void setUp(EmbeddedKafkaBroker broker) {
    kluster = new Kluster(
      Admin.create(
        Map.of(
          "bootstrap.servers",
          broker.getBrokersAsString())));

    resolveTopikParameters.setUp(kluster);

    var configs = KafkaTestUtils.producerProps(broker);
    configs.put("acks", "all");
    intStrProducer = new KafkaProducer<>(configs);
  }

  @AfterEach
  void tearDown() {
    intStrProducer.close();
    resolveTopikParameters.tearDown();
    kluster.close();
  }
}
