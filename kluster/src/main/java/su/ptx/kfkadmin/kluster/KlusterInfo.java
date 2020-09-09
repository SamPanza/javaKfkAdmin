package su.ptx.kfkadmin.kluster;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.Node;

import java.util.Collection;

@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public final class KlusterInfo {
  private final DescribeClusterResult dcr;

  @SneakyThrows
  public String id() {
    return dcr.clusterId().get();
  }

  @SneakyThrows
  public Node controller() {
    return dcr.controller().get();
  }

  @SneakyThrows
  public Collection<Node> nodes() {
    return dcr.nodes().get();
  }
}
