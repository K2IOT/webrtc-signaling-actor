package io.webrtc.signaling.observability;

import java.util.Objects;
import java.util.concurrent.*;
import java.util.function.*;

/** Captured bounded observation context; delegates lifecycle, exceptions and physical ownership. */
public final class ContextualStage<T> implements CompletionStage<T> {
  private final CompletionStage<T> delegate;
  private final TraceContext context;

  private ContextualStage(CompletionStage<T> delegate, TraceContext context) {
    this.delegate = delegate;
    this.context = context;
  }

  public static <T> CompletionStage<T> capture(CompletionStage<T> stage) {
    if (stage instanceof ContextualStage<?>) return stage;
    var context = TraceContext.current();
    return context == null ? stage : new ContextualStage<>(stage, context);
  }

  public static <T> CompletionStage<T> readOnly(CompletionStage<T> stage) {
    var context =
        stage instanceof ContextualStage<?> wrapped ? wrapped.context : TraceContext.current();
    var original = stage.toCompletableFuture().minimalCompletionStage();
    return context == null ? original : new ContextualStage<>(original, context);
  }

  private <U> CompletionStage<U> wrap(CompletionStage<U> stage) {
    return new ContextualStage<>(stage, context);
  }

  private <A, B> Function<A, B> scoped(Function<A, B> fn) {
    Objects.requireNonNull(fn);
    return value -> {
      try (var ignored = TraceContext.activate(context)) {
        return fn.apply(value);
      }
    };
  }

  private <A, B, R> BiFunction<A, B, R> scoped(BiFunction<A, B, R> fn) {
    Objects.requireNonNull(fn);
    return (a, b) -> {
      try (var ignored = TraceContext.activate(context)) {
        return fn.apply(a, b);
      }
    };
  }

  private <A> Consumer<A> scoped(Consumer<A> fn) {
    Objects.requireNonNull(fn);
    return value -> {
      try (var ignored = TraceContext.activate(context)) {
        fn.accept(value);
      }
    };
  }

  private <A, B> BiConsumer<A, B> scoped(BiConsumer<A, B> fn) {
    Objects.requireNonNull(fn);
    return (a, b) -> {
      try (var ignored = TraceContext.activate(context)) {
        fn.accept(a, b);
      }
    };
  }

  private Runnable scoped(Runnable fn) {
    Objects.requireNonNull(fn);
    return () -> {
      try (var ignored = TraceContext.activate(context)) {
        fn.run();
      }
    };
  }

  @Override
  public <U> CompletionStage<U> thenApply(Function<? super T, ? extends U> fn) {
    return wrap(delegate.thenApply(scoped(fn)));
  }

  @Override
  public <U> CompletionStage<U> thenApplyAsync(Function<? super T, ? extends U> fn) {
    return wrap(delegate.thenApplyAsync(scoped(fn)));
  }

  @Override
  public <U> CompletionStage<U> thenApplyAsync(
      Function<? super T, ? extends U> fn, Executor executor) {
    return wrap(delegate.thenApplyAsync(scoped(fn), executor));
  }

  @Override
  public CompletionStage<Void> thenAccept(Consumer<? super T> fn) {
    return wrap(delegate.thenAccept(scoped(fn)));
  }

  @Override
  public CompletionStage<Void> thenAcceptAsync(Consumer<? super T> fn) {
    return wrap(delegate.thenAcceptAsync(scoped(fn)));
  }

  @Override
  public CompletionStage<Void> thenAcceptAsync(Consumer<? super T> fn, Executor executor) {
    return wrap(delegate.thenAcceptAsync(scoped(fn), executor));
  }

  @Override
  public CompletionStage<Void> thenRun(Runnable fn) {
    return wrap(delegate.thenRun(scoped(fn)));
  }

  @Override
  public CompletionStage<Void> thenRunAsync(Runnable fn) {
    return wrap(delegate.thenRunAsync(scoped(fn)));
  }

  @Override
  public CompletionStage<Void> thenRunAsync(Runnable fn, Executor executor) {
    return wrap(delegate.thenRunAsync(scoped(fn), executor));
  }

  @Override
  public <U, V> CompletionStage<V> thenCombine(
      CompletionStage<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn) {
    return wrap(delegate.thenCombine(other, scoped(fn)));
  }

  @Override
  public <U, V> CompletionStage<V> thenCombineAsync(
      CompletionStage<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn) {
    return wrap(delegate.thenCombineAsync(other, scoped(fn)));
  }

  @Override
  public <U, V> CompletionStage<V> thenCombineAsync(
      CompletionStage<? extends U> other,
      BiFunction<? super T, ? super U, ? extends V> fn,
      Executor executor) {
    return wrap(delegate.thenCombineAsync(other, scoped(fn), executor));
  }

  @Override
  public <U> CompletionStage<Void> thenAcceptBoth(
      CompletionStage<? extends U> other, BiConsumer<? super T, ? super U> fn) {
    return wrap(delegate.thenAcceptBoth(other, scoped(fn)));
  }

  @Override
  public <U> CompletionStage<Void> thenAcceptBothAsync(
      CompletionStage<? extends U> other, BiConsumer<? super T, ? super U> fn) {
    return wrap(delegate.thenAcceptBothAsync(other, scoped(fn)));
  }

  @Override
  public <U> CompletionStage<Void> thenAcceptBothAsync(
      CompletionStage<? extends U> other, BiConsumer<? super T, ? super U> fn, Executor executor) {
    return wrap(delegate.thenAcceptBothAsync(other, scoped(fn), executor));
  }

  @Override
  public CompletionStage<Void> runAfterBoth(CompletionStage<?> other, Runnable fn) {
    return wrap(delegate.runAfterBoth(other, scoped(fn)));
  }

  @Override
  public CompletionStage<Void> runAfterBothAsync(CompletionStage<?> other, Runnable fn) {
    return wrap(delegate.runAfterBothAsync(other, scoped(fn)));
  }

  @Override
  public CompletionStage<Void> runAfterBothAsync(
      CompletionStage<?> other, Runnable fn, Executor executor) {
    return wrap(delegate.runAfterBothAsync(other, scoped(fn), executor));
  }

  @Override
  public <U> CompletionStage<U> applyToEither(
      CompletionStage<? extends T> other, Function<? super T, U> fn) {
    return wrap(delegate.applyToEither(other, scoped(fn)));
  }

  @Override
  public <U> CompletionStage<U> applyToEitherAsync(
      CompletionStage<? extends T> other, Function<? super T, U> fn) {
    return wrap(delegate.applyToEitherAsync(other, scoped(fn)));
  }

  @Override
  public <U> CompletionStage<U> applyToEitherAsync(
      CompletionStage<? extends T> other, Function<? super T, U> fn, Executor executor) {
    return wrap(delegate.applyToEitherAsync(other, scoped(fn), executor));
  }

  @Override
  public CompletionStage<Void> acceptEither(
      CompletionStage<? extends T> other, Consumer<? super T> fn) {
    return wrap(delegate.acceptEither(other, scoped(fn)));
  }

  @Override
  public CompletionStage<Void> acceptEitherAsync(
      CompletionStage<? extends T> other, Consumer<? super T> fn) {
    return wrap(delegate.acceptEitherAsync(other, scoped(fn)));
  }

  @Override
  public CompletionStage<Void> acceptEitherAsync(
      CompletionStage<? extends T> other, Consumer<? super T> fn, Executor executor) {
    return wrap(delegate.acceptEitherAsync(other, scoped(fn), executor));
  }

  @Override
  public CompletionStage<Void> runAfterEither(CompletionStage<?> other, Runnable fn) {
    return wrap(delegate.runAfterEither(other, scoped(fn)));
  }

  @Override
  public CompletionStage<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable fn) {
    return wrap(delegate.runAfterEitherAsync(other, scoped(fn)));
  }

  @Override
  public CompletionStage<Void> runAfterEitherAsync(
      CompletionStage<?> other, Runnable fn, Executor executor) {
    return wrap(delegate.runAfterEitherAsync(other, scoped(fn), executor));
  }

  @Override
  public <U> CompletionStage<U> thenCompose(Function<? super T, ? extends CompletionStage<U>> fn) {
    return wrap(delegate.thenCompose(scoped(fn)));
  }

  @Override
  public <U> CompletionStage<U> thenComposeAsync(
      Function<? super T, ? extends CompletionStage<U>> fn) {
    return wrap(delegate.thenComposeAsync(scoped(fn)));
  }

  @Override
  public <U> CompletionStage<U> thenComposeAsync(
      Function<? super T, ? extends CompletionStage<U>> fn, Executor executor) {
    return wrap(delegate.thenComposeAsync(scoped(fn), executor));
  }

  @Override
  public <U> CompletionStage<U> handle(BiFunction<? super T, Throwable, ? extends U> fn) {
    return wrap(delegate.handle(scoped(fn)));
  }

  @Override
  public <U> CompletionStage<U> handleAsync(BiFunction<? super T, Throwable, ? extends U> fn) {
    return wrap(delegate.handleAsync(scoped(fn)));
  }

  @Override
  public <U> CompletionStage<U> handleAsync(
      BiFunction<? super T, Throwable, ? extends U> fn, Executor executor) {
    return wrap(delegate.handleAsync(scoped(fn), executor));
  }

  @Override
  public CompletionStage<T> whenComplete(BiConsumer<? super T, ? super Throwable> fn) {
    return wrap(delegate.whenComplete(scoped(fn)));
  }

  @Override
  public CompletionStage<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> fn) {
    return wrap(delegate.whenCompleteAsync(scoped(fn)));
  }

  @Override
  public CompletionStage<T> whenCompleteAsync(
      BiConsumer<? super T, ? super Throwable> fn, Executor executor) {
    return wrap(delegate.whenCompleteAsync(scoped(fn), executor));
  }

  @Override
  public CompletionStage<T> exceptionally(Function<Throwable, ? extends T> fn) {
    return wrap(delegate.exceptionally(scoped(fn)));
  }

  @Override
  public CompletionStage<T> exceptionallyAsync(Function<Throwable, ? extends T> fn) {
    return wrap(delegate.exceptionallyAsync(scoped(fn)));
  }

  @Override
  public CompletionStage<T> exceptionallyAsync(
      Function<Throwable, ? extends T> fn, Executor executor) {
    return wrap(delegate.exceptionallyAsync(scoped(fn), executor));
  }

  @Override
  public CompletionStage<T> exceptionallyCompose(
      Function<Throwable, ? extends CompletionStage<T>> fn) {
    return wrap(delegate.exceptionallyCompose(scoped(fn)));
  }

  @Override
  public CompletionStage<T> exceptionallyComposeAsync(
      Function<Throwable, ? extends CompletionStage<T>> fn) {
    return wrap(delegate.exceptionallyComposeAsync(scoped(fn)));
  }

  @Override
  public CompletionStage<T> exceptionallyComposeAsync(
      Function<Throwable, ? extends CompletionStage<T>> fn, Executor executor) {
    return wrap(delegate.exceptionallyComposeAsync(scoped(fn), executor));
  }

  @Override
  public CompletableFuture<T> toCompletableFuture() {
    return delegate.toCompletableFuture();
  }
}
