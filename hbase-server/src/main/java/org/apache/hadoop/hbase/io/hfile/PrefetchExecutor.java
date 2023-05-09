/*
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
package org.apache.hadoop.hbase.io.hfile;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.PersistentPrefetchProtos;

@InterfaceAudience.Private
public final class PrefetchExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(PrefetchExecutor.class);

  /** Futures for tracking block prefetch activity */
  private static final Map<Path, Future<?>> prefetchFutures = new ConcurrentSkipListMap<>();
  /** Set of files for which prefetch is completed */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "MS_SHOULD_BE_FINAL")
  private static HashMap<String, Boolean> prefetchCompleted = new HashMap<>();
  /** Executor pool shared among all HFiles for block prefetch */
  private static final ScheduledExecutorService prefetchExecutorPool;
  /** Delay before beginning prefetch */
  private static final int prefetchDelayMillis;
  /** Variation in prefetch delay times, to mitigate stampedes */
  private static final float prefetchDelayVariation;
  static String prefetchedFileListPath;
  static {
    // Consider doing this on demand with a configuration passed in rather
    // than in a static initializer.
    Configuration conf = HBaseConfiguration.create();
    // 1s here for tests, consider 30s in hbase-default.xml
    // Set to 0 for no delay
    prefetchDelayMillis = conf.getInt("hbase.hfile.prefetch.delay", 1000);
    prefetchDelayVariation = conf.getFloat("hbase.hfile.prefetch.delay.variation", 0.2f);
    int prefetchThreads = conf.getInt("hbase.hfile.thread.prefetch", 4);
    prefetchExecutorPool = new ScheduledThreadPoolExecutor(prefetchThreads, new ThreadFactory() {
      @Override
      public Thread newThread(Runnable r) {
        String name = "hfile-prefetch-" + EnvironmentEdgeManager.currentTime();
        Thread t = new Thread(r, name);
        t.setDaemon(true);
        return t;
      }
    });
  }

  // TODO: We want HFile, which is where the blockcache lives, to handle
  // prefetching of file blocks but the Store level is where path convention
  // knowledge should be contained
  private static final Pattern prefetchPathExclude =
    Pattern.compile("(" + Path.SEPARATOR_CHAR + HConstants.HBASE_TEMP_DIRECTORY.replace(".", "\\.")
      + Path.SEPARATOR_CHAR + ")|(" + Path.SEPARATOR_CHAR
      + HConstants.HREGION_COMPACTIONDIR_NAME.replace(".", "\\.") + Path.SEPARATOR_CHAR + ")");

  public static void request(Path path, Runnable runnable) {
    if (prefetchCompleted != null) {
      if (isFilePrefetched(path.getName())) {
        LOG.info(
          "File has already been prefetched before the restart, so skipping prefetch : " + path);
        return;
      }
    }
    if (!prefetchPathExclude.matcher(path.toString()).find()) {
      long delay;
      if (prefetchDelayMillis > 0) {
        delay = (long) ((prefetchDelayMillis * (1.0f - (prefetchDelayVariation / 2)))
          + (prefetchDelayMillis * (prefetchDelayVariation / 2)
            * ThreadLocalRandom.current().nextFloat()));
      } else {
        delay = 0;
      }
      try {
        LOG.debug("Prefetch requested for {}, delay={} ms", path, delay);
        final Runnable tracedRunnable =
          TraceUtil.tracedRunnable(runnable, "PrefetchExecutor.request");
        final Future<?> future =
          prefetchExecutorPool.schedule(tracedRunnable, delay, TimeUnit.MILLISECONDS);
        prefetchFutures.put(path, future);
      } catch (RejectedExecutionException e) {
        prefetchFutures.remove(path);
        LOG.warn("Prefetch request rejected for {}", path);
      }
    }
  }

  public static void complete(Path path) {
    prefetchFutures.remove(path);
    prefetchCompleted.put(path.getName(), true);
    LOG.debug("Prefetch completed for {}", path.getName());
  }

  public static void cancel(Path path) {
    Future<?> future = prefetchFutures.get(path);
    if (future != null) {
      // ok to race with other cancellation attempts
      future.cancel(true);
      prefetchFutures.remove(path);
      LOG.debug("Prefetch cancelled for {}", path);
    }
    LOG.debug("Removing filename from the prefetched persistence list: {}", path.getName());
    removePrefetchedFileWhileEvict(path.getName());
  }

  public static boolean isCompleted(Path path) {
    Future<?> future = prefetchFutures.get(path);
    if (future != null) {
      return future.isDone();
    }
    return true;
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "OBL_UNSATISFIED_OBLIGATION",
      justification = "false positive, try-with-resources ensures close is called.")
  public static void persistToFile(String path) throws IOException {
    prefetchedFileListPath = path;
    if (prefetchedFileListPath == null) {
      LOG.info("Exception while persisting prefetch!");
      throw new IOException("Error persisting prefetched HFiles set!");
    }
    if (!prefetchCompleted.isEmpty()) {
      try (FileOutputStream fos = new FileOutputStream(prefetchedFileListPath, false)) {
        PrefetchProtoUtils.toPB(prefetchCompleted).writeDelimitedTo(fos);
      }
    }
  }

  public static void retrieveFromFile(String path) throws IOException {
    prefetchedFileListPath = path;
    File prefetchPersistenceFile = new File(prefetchedFileListPath);
    if (!prefetchPersistenceFile.exists()) {
      LOG.warn("Prefetch persistence file does not exist!");
      return;
    }
    LOG.info("Retrieving from prefetch persistence file " + path);
    assert (prefetchedFileListPath != null);
    try (FileInputStream fis = deleteFileOnClose(prefetchPersistenceFile)) {
      PersistentPrefetchProtos.PrefetchedHfileName proto =
        PersistentPrefetchProtos.PrefetchedHfileName.parseDelimitedFrom(fis);
      Map<String, Boolean> protoPrefetchedFilesMap = proto.getPrefetchedFilesMap();
      prefetchCompleted.putAll(protoPrefetchedFilesMap);
    }
  }

  private static FileInputStream deleteFileOnClose(final File file) throws IOException {
    return new FileInputStream(file) {
      private File myFile;

      private FileInputStream init(File file) {
        myFile = file;
        return this;
      }

      @Override
      public void close() throws IOException {
        if (myFile == null) {
          return;
        }

        super.close();
        if (!myFile.delete()) {
          throw new IOException("Failed deleting persistence file " + myFile.getAbsolutePath());
        }
        myFile = null;
      }
    }.init(file);
  }

  public static void removePrefetchedFileWhileEvict(String hfileName) {
    prefetchCompleted.remove(hfileName);
  }

  public static boolean isFilePrefetched(String hfileName) {
    return prefetchCompleted.containsKey(hfileName);
  }

  private PrefetchExecutor() {
  }
}
