package org.apache.hadoop.hbase.io.hfile;

import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.DaemonThreadFactory;

public class PreloadThreadPool {
  public static final Log LOG = LogFactory.getLog(PreloadThreadPool.class);

  private static PreloadThreadPool pool;

  private ThreadPoolExecutor preloadThreadsPool;

  private PreloadThreadPool(int minimum, int maximum) {
    // A new thread pool with the following policy
    // If a task is submitted and number of threads <= maximum
    preloadThreadsPool =
        new ThreadPoolExecutor(minimum, maximum, 360, TimeUnit.SECONDS,
            new SynchronousQueue<Runnable>(), new DaemonThreadFactory("scan-preloader-"),
            new ThreadPoolExecutor.AbortPolicy());
    preloadThreadsPool.allowCoreThreadTimeOut(true);
  }

  public static PreloadThreadPool getThreadPool() {
    return pool;
  }

  public synchronized static void constructPreloaderThreadPool(int min, int max) {
    if (pool == null) {
      pool = new PreloadThreadPool(min, max);
    }
  }

  public Future runTask(Runnable task) {
    try {
      return preloadThreadsPool.submit(task);
    } catch (RejectedExecutionException e) {
      LOG.debug("Execution of preloader refused");
    }
    return null;
  }
}
