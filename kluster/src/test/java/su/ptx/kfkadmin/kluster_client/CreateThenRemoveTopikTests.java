package su.ptx.kfkadmin.kluster_client;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.test.context.EmbeddedKafka;
import su.ptx.kfkadmin.kluster.Kluster;

import static org.junit.jupiter.api.Assertions.assertTrue;

@EmbeddedKafka(partitions = 1)
final class CreateThenRemoveTopikTests {
  private static final Kluster KLUSTER = new KlusterSupplier().get();

  @AfterAll
  static void closeKLUSTER() {
    KLUSTER.close();
  }

  @Test
  void createThenRemove() {
    var foo = KLUSTER.createTopik("baz");
    assertTrue(KLUSTER.topiks().contains(foo));
    //TODO remove topic
  }
}
