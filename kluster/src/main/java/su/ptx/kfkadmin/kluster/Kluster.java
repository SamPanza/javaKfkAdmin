package su.ptx.kfkadmin.kluster;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.experimental.Accessors;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.*;

import static java.util.Collections.singletonMap;
import static java.util.Optional.ofNullable;
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

  public Topik createTopik(String name) {
    return createTopik(name, null, null);
  }

  public Topik createTopik(String name, int numPartitions) {
    return createTopik(name, numPartitions, null);
  }

  public Topik createTopik(String name, int numPartitions, short replicationFactor) {
    return createTopik(name, Integer.valueOf(numPartitions), Short.valueOf(replicationFactor));
  }

  @SneakyThrows
  private Topik createTopik(String name, Integer numPartitions, Short replicationFactor) {
    admin.createTopics(
      Set.of(
        new NewTopic(name,
          ofNullable(numPartitions),
          ofNullable(replicationFactor))))
      .all()
      .get();
    return topik(name);
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

    public long[] clear() {
      return partitions().stream().mapToLong(Partition::clear).toArray();
    }

    @SneakyThrows
    public void delete() {
      admin.deleteTopics(Set.of(name)).all().get();
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

      public List<Node> replicas() {
        return tpi.replicas();
      }

      public long size() {
        return latestOffset().value() - earliestOffset().value();
      }

      @SneakyThrows
      public long clear() {
        return admin.deleteRecords(
          Map.of(
            topicPartition(),
            RecordsToDelete.beforeOffset(latestOffset().value())))
          .lowWatermarks()
          .get(topicPartition())
          .get()
          .lowWatermark();
      }

      public Offset earliestOffset() {
        return offset(OffsetSpec.earliest());
      }

      public Offset latestOffset() {
        return offset(OffsetSpec.latest());
      }

      @SneakyThrows
      private Offset offset(OffsetSpec spec) {
        return new Offset(admin.listOffsets(singletonMap(topicPartition(), spec)).all().get().get(topicPartition()));
      }

      private TopicPartition topicPartition() {
        return new TopicPartition(name, id());
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
