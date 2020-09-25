package su.ptx.kfkadmin.kluster;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.experimental.Accessors;
import org.apache.kafka.clients.admin.Admin;

import java.util.SortedSet;
import java.util.TreeSet;

import static java.util.stream.Collectors.toCollection;

@RequiredArgsConstructor
public final class Kluster implements AutoCloseable {
  private final Admin admin;

  @Override
  public void close() {
    admin.close();
  }

  public KlusterInfo info() {
    return new KlusterInfo(admin.describeCluster());
  }

  @SneakyThrows
  public SortedSet<Topik> topiks() {
    return admin.listTopics().names().get().stream().map(Topik::new).collect(toCollection(TreeSet::new));
  }

  @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
  public final class Topik implements Comparable<Topik> {
    @Getter
    @Accessors(fluent = true)
    private final String name;

    @Override
    public int compareTo(Topik that) {
      return name.compareTo(that.name);
    }
  }
}
