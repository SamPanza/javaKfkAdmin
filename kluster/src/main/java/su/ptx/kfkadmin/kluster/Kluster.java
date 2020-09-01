package su.ptx.kfkadmin.kluster;

import org.apache.kafka.clients.admin.Admin;

public final class Kluster implements AutoCloseable {
  private final Admin admin;

  public Kluster(Admin admin) {
    this.admin = admin;
  }

  @Override
  public void close() {
    admin.close();
  }
}
