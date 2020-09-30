package su.ptx.kfkadmin.kluster;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.experimental.Accessors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import static java.util.Collections.singletonMap;
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
    return admin.listTopics().names()
      .get()
      .stream()
      .map(Topik::new)
      .collect(toCollection(TreeSet::new));
  }

  public Topik topik(String name) {
    return topiks()
      .stream()
      .filter(topik -> topik.name.equals(name))
      .findFirst()
      //TODO specific exc, not NSEE
      .orElseThrow();
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

    @SneakyThrows
    public SortedSet<Partition> partitions() {
      return admin.describeTopics(Set.of(name))
        .all()
        .get()
        .get(name)
        .partitions()
        .stream()
        .map(Partition::new)
        .collect(toCollection(TreeSet::new));
    }

    public long size() {
      return partitions().stream().mapToLong(Partition::size).sum();
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public final class Partition implements Comparable<Partition> {
      private final TopicPartitionInfo tpi;

      @Override
      public int compareTo(Partition that) {
        return id() - that.id();
      }

      public int id() {
        return tpi.partition();
      }

      public long size() {
        return latestOffset().value() - earliestOffset().value();
      }

      public Offset earliestOffset() {
        return offset(OffsetSpec.earliest());
      }

      public Offset latestOffset() {
        return offset(OffsetSpec.latest());
      }

      @SneakyThrows
      private Offset offset(OffsetSpec spec) {
        var tp = new TopicPartition(name, id());
        return new Offset(admin.listOffsets(singletonMap(tp, spec)).all().get().get(tp));
      }

      @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
      public final class Offset {
        private final ListOffsetsResult.ListOffsetsResultInfo lori;

        public final long value() {
          return lori.offset();
        }
      }
    }
  }
}
