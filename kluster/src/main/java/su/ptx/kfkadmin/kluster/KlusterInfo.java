package su.ptx.kfkadmin.kluster;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.apache.kafka.clients.admin.DescribeClusterResult;

@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public final class KlusterInfo {
  private final DescribeClusterResult dcr;

  @SneakyThrows
  public String id() {
    return dcr.clusterId().get();
  }
}
