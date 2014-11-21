package org.apache.hadoop.hbase.util.serial;

import java.util.concurrent.Callable;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * Executor service which allows creation of arbitrary number of serial streams, all to be executed
 * on a single thread pool. All commands scheduled on a single serial stream are executed serially
 * (not necessarily on the same thread).
 */
public interface SerialExecutorService {
  /** Creates new serial stream */
  SerialExecutionStream createStream();

  /**
   * Executor, that makes sure commands are executed serially, in
   * order they are scheduled. No two commands will be running at the same time.
   */
  public interface SerialExecutionStream {
    /**
     * Executes the given command on this stream, returning future.
     * If future command return is not null, next command in the stream
     * will not be executed before future completes.
     *
     * I.e. async commands should return future when it completes,
     * and non-async commands should return null.
     *
     * @return Returns future which is going to be completed when command and
     *   future it returns both finish.
     */
    ListenableFuture<Void> execute(Callable<ListenableFuture<Void>> command);
  }
}
