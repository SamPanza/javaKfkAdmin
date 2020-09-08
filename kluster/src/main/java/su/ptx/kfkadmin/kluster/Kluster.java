package su.ptx.kfkadmin.kluster;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.Admin;

@RequiredArgsConstructor
public final class Kluster implements AutoCloseable {
  private final Admin admin;

  public KlusterInfo info() {
    return new KlusterInfo(admin.describeCluster());
  }

  @Override
  public void close() {
    admin.close();
  }
}
