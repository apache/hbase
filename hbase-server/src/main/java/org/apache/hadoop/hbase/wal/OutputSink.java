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
package org.apache.hadoop.hbase.wal;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

/**
 * The following class is an abstraction class to provide a common interface to support different
 * ways of consuming recovered edits.
 */
@InterfaceAudience.Private
public abstract class OutputSink {
  private static final Logger LOG = LoggerFactory.getLogger(OutputSink.class);

  private final WALSplitter.PipelineController controller;
  protected final EntryBuffers entryBuffers;

  private final List<WriterThread> writerThreads = Lists.newArrayList();

  protected final int numThreads;

  protected CancelableProgressable reporter = null;

  protected final AtomicLong totalSkippedEdits = new AtomicLong();

  /**
   * List of all the files produced by this sink
   */
  protected final List<Path> splits = new ArrayList<>();

  protected MonitoredTask status = null;

  /**
   * Used when close this output sink.
   */
  protected final ThreadPoolExecutor closeThreadPool;
  protected final CompletionService<Void> closeCompletionService;

  public OutputSink(WALSplitter.PipelineController controller, EntryBuffers entryBuffers,
      int numWriters) {
    this.numThreads = numWriters;
    this.controller = controller;
    this.entryBuffers = entryBuffers;
    this.closeThreadPool = Threads.getBoundedCachedThreadPool(numThreads, 30L, TimeUnit.SECONDS,
      new ThreadFactoryBuilder().setNameFormat("split-log-closeStream-pool-%d").setDaemon(true)
        .setUncaughtExceptionHandler(Threads.LOGGING_EXCEPTION_HANDLER).build());
    this.closeCompletionService = new ExecutorCompletionService<>(closeThreadPool);
  }

  void setReporter(CancelableProgressable reporter) {
    this.reporter = reporter;
  }

  void setStatus(MonitoredTask status) {
    this.status = status;
  }

  /**
   * Start the threads that will pump data from the entryBuffers to the output files.
   */
  public void startWriterThreads() throws IOException {
    for (int i = 0; i < numThreads; i++) {
      WriterThread t = new WriterThread(controller, entryBuffers, this, i);
      t.start();
      writerThreads.add(t);
    }
  }

  public synchronized void restartWriterThreadsIfNeeded() {
    for(int i = 0; i< writerThreads.size(); i++){
      WriterThread t = writerThreads.get(i);
      if (!t.isAlive()){
        String threadName = t.getName();
        LOG.debug("Replacing dead thread: " + threadName);
        WriterThread newThread = new WriterThread(controller, entryBuffers, this, threadName);
        newThread.start();
        writerThreads.set(i, newThread);
      }
    }
  }

  /**
   * Wait for writer threads to dump all info to the sink
   *
   * @return true when there is no error
   */
  protected boolean finishWriterThreads(boolean interrupt) throws IOException {
    LOG.debug("Waiting for split writer threads to finish");
    boolean progressFailed = false;
    for (WriterThread t : writerThreads) {
      t.finish();
    }
    if (interrupt) {
      for (WriterThread t : writerThreads) {
        t.interrupt(); // interrupt the writer threads. We are stopping now.
      }
    }

    for (WriterThread t : writerThreads) {
      if (!progressFailed && reporter != null && !reporter.progress()) {
        progressFailed = true;
      }
      try {
        t.join();
      } catch (InterruptedException ie) {
        IOException iie = new InterruptedIOException();
        iie.initCause(ie);
        throw iie;
      }
    }
    controller.checkForErrors();
    final String msg = this.writerThreads.size() + " split writer threads finished";
    LOG.info(msg);
    updateStatusWithMsg(msg);
    return (!progressFailed);
  }

  long getTotalSkippedEdits() {
    return this.totalSkippedEdits.get();
  }

  /**
   * @return the number of currently opened writers
   */
  protected abstract int getNumOpenWriters();

  /**
   * @param buffer A buffer of some number of edits for a given region.
   * @throws IOException For any IO errors
   */
  protected abstract void append(EntryBuffers.RegionEntryBuffer buffer) throws IOException;

  protected abstract List<Path> close() throws IOException;

  /**
   * @return a map from encoded region ID to the number of edits written out for that region.
   */
  protected abstract Map<String, Long> getOutputCounts();

  /**
   * @return number of regions we've recovered
   */
  protected abstract int getNumberOfRecoveredRegions();

  /**
   * Some WALEdit's contain only KV's for account on what happened to a region. Not all sinks will
   * want to get all of those edits.
   * @return Return true if this sink wants to accept this region-level WALEdit.
   */
  protected abstract boolean keepRegionEvent(WAL.Entry entry);

  /**
   * Set status message in {@link MonitoredTask} instance that is set in this OutputSink
   * @param msg message to update the status with
   */
  protected final void updateStatusWithMsg(String msg) {
    if (status != null) {
      status.setStatus(msg);
    }
  }

  public static class WriterThread extends Thread {
    private volatile boolean shouldStop = false;
    private WALSplitter.PipelineController controller;
    private EntryBuffers entryBuffers;
    private OutputSink outputSink = null;

    WriterThread(WALSplitter.PipelineController controller, EntryBuffers entryBuffers,
        OutputSink sink, int i) {
      this(controller, entryBuffers, sink, Thread.currentThread().getName() + "-Writer-" + i);
    }

    WriterThread(WALSplitter.PipelineController controller, EntryBuffers entryBuffers,
        OutputSink sink, String threadName) {
      super(threadName);
      this.controller = controller;
      this.entryBuffers = entryBuffers;
      outputSink = sink;
    }

    @Override
    public void run()  {
      try {
        doRun();
      } catch (Throwable t) {
        LOG.error("Exiting thread", t);
        controller.writerThreadError(t);
      }
    }

    private void doRun() throws IOException {
      LOG.trace("Writer thread starting");
      while (true) {
        EntryBuffers.RegionEntryBuffer buffer = entryBuffers.getChunkToWrite();
        if (buffer == null) {
          // No data currently available, wait on some more to show up
          synchronized (controller.dataAvailable) {
            if (shouldStop) {
              return;
            }
            try {
              controller.dataAvailable.wait(500);
            } catch (InterruptedException ie) {
              if (!shouldStop) {
                throw new RuntimeException(ie);
              }
            }
          }
          continue;
        }

        assert buffer != null;
        try {
          writeBuffer(buffer);
        } finally {
          entryBuffers.doneWriting(buffer);
        }
      }
    }

    private void writeBuffer(EntryBuffers.RegionEntryBuffer buffer) throws IOException {
      outputSink.append(buffer);
    }

    private void finish() {
      synchronized (controller.dataAvailable) {
        shouldStop = true;
        controller.dataAvailable.notifyAll();
      }
    }
  }
}
