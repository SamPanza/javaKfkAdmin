package su.ptx.kfkadmin.kluster_client;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import su.ptx.kfkadmin.kluster.Kluster;

import java.util.Map;
import java.util.function.Supplier;

final class KlusterSupplier implements Supplier<Kluster> {
  @Override
  public Kluster get() {
    return new Kluster(
      Admin.create(
        Map.of(
          CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
          System.getProperty(EmbeddedKafkaBroker.SPRING_EMBEDDED_KAFKA_BROKERS))));
  }
}
