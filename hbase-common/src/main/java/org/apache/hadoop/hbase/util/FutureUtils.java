/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.util;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import org.apache.hadoop.hbase.exceptions.TimeoutIOException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Throwables;

/**
 * Helper class for processing futures.
 */
@InterfaceAudience.Private
public final class FutureUtils {

  private static final Logger LOG = LoggerFactory.getLogger(FutureUtils.class);

  private FutureUtils() {
  }

  /**
   * This is method is used when you just want to add a listener to the given future. We will call
   * {@link CompletableFuture#whenComplete(BiConsumer)} to register the {@code action} to the
   * {@code future}. Ignoring the return value of a Future is considered as a bad practice as it may
   * suppress exceptions thrown from the code that completes the future, and this method will catch
   * all the exception thrown from the {@code action} to catch possible code bugs.
   * <p/>
   * And the error phone check will always report FutureReturnValueIgnored because every method in
   * the {@link CompletableFuture} class will return a new {@link CompletableFuture}, so you always
   * have one future that has not been checked. So we introduce this method and add a suppress
   * warnings annotation here.
   */
  @SuppressWarnings("FutureReturnValueIgnored")
  public static <T> void addListener(CompletableFuture<T> future,
      BiConsumer<? super T, ? super Throwable> action) {
    future.whenComplete((resp, error) -> {
      try {
        // See this post on stack overflow(shorten since the url is too long),
        // https://s.apache.org/completionexception
        // For a chain of CompleableFuture, only the first child CompletableFuture can get the
        // original exception, others will get a CompletionException, which wraps the original
        // exception. So here we unwrap it before passing it to the callback action.
        action.accept(resp, unwrapCompletionException(error));
      } catch (Throwable t) {
        LOG.error("Unexpected error caught when processing CompletableFuture", t);
      }
    });
  }

  /**
   * Almost the same with {@link #addListener(CompletableFuture, BiConsumer)} method above, the only
   * exception is that we will call
   * {@link CompletableFuture#whenCompleteAsync(BiConsumer, Executor)}.
   * @see #addListener(CompletableFuture, BiConsumer)
   */
  @SuppressWarnings("FutureReturnValueIgnored")
  public static <T> void addListener(CompletableFuture<T> future,
      BiConsumer<? super T, ? super Throwable> action, Executor executor) {
    future.whenCompleteAsync((resp, error) -> {
      try {
        action.accept(resp, unwrapCompletionException(error));
      } catch (Throwable t) {
        LOG.error("Unexpected error caught when processing CompletableFuture", t);
      }
    }, executor);
  }

  /**
   * Return a {@link CompletableFuture} which is same with the given {@code future}, but execute all
   * the callbacks in the given {@code executor}.
   */
  public static <T> CompletableFuture<T> wrapFuture(CompletableFuture<T> future,
      Executor executor) {
    CompletableFuture<T> wrappedFuture = new CompletableFuture<>();
    addListener(future, (r, e) -> {
      if (e != null) {
        wrappedFuture.completeExceptionally(e);
      } else {
        wrappedFuture.complete(r);
      }
    }, executor);
    return wrappedFuture;
  }

  /**
   * Get the cause of the {@link Throwable} if it is a {@link CompletionException}.
   */
  public static Throwable unwrapCompletionException(Throwable error) {
    if (error instanceof CompletionException) {
      Throwable cause = error.getCause();
      if (cause != null) {
        return cause;
      }
    }
    return error;
  }

  /**
   * A helper class for getting the result of a Future, and convert the error to an
   * {@link IOException}.
   */
  public static <T> T get(Future<T> future) throws IOException {
    try {
      return future.get();
    } catch (InterruptedException e) {
      throw (IOException) new InterruptedIOException().initCause(e);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      Throwables.propagateIfPossible(cause, IOException.class);
      throw new IOException(cause);
    }
  }

  /**
   * A helper class for getting the result of a Future with timeout, and convert the error to an
   * {@link IOException}.
   */
  public static <T> T get(Future<T> future, long timeout, TimeUnit unit) throws IOException {
    try {
      return future.get(timeout, unit);
    } catch (InterruptedException e) {
      throw (IOException) new InterruptedIOException().initCause(e);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      Throwables.propagateIfPossible(cause, IOException.class);
      throw new IOException(cause);
    } catch (TimeoutException e) {
      throw new TimeoutIOException(e);
    }
  }

  /**
   * Returns a CompletableFuture that is already completed exceptionally with the given exception.
   */
  public static <T> CompletableFuture<T> failedFuture(Throwable e) {
    CompletableFuture<T> future = new CompletableFuture<>();
    future.completeExceptionally(e);
    return future;
  }
}
