package su.ptx.kfkadmin.kluster_client;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import su.ptx.kfkadmin.kluster.Kluster;

import java.util.Map;

final class KlusterHolder {
  private static Kluster kluster;

  private KlusterHolder() {
    //
  }

  static Kluster init() {
    assert kluster == null;
    return kluster = new Kluster(
      Admin.create(
        Map.of(
          CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
          System.getProperty(EmbeddedKafkaBroker.SPRING_EMBEDDED_KAFKA_BROKERS))));
  }

  static void destroy() {
    assert kluster != null;
    kluster.close();
  }
}
