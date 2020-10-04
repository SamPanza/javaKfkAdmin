package su.ptx;

import lombok.SneakyThrows;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;

public final class GetFuture<T> implements Function<Future<? extends T>, T> {
  @Override
  @SneakyThrows({InterruptedException.class, ExecutionException.class})
  public T apply(Future<? extends T> future) {
    return future.get();
  }
}
