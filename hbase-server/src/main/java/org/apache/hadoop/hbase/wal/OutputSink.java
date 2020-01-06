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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CancelableProgressable;
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

  protected WALSplitter.PipelineController controller;
  protected EntryBuffers entryBuffers;

  protected ConcurrentHashMap<String, WALSplitter.SinkWriter> writers = new ConcurrentHashMap<>();
  protected final ConcurrentHashMap<String, Long> regionMaximumEditLogSeqNum =
      new ConcurrentHashMap<>();

  protected final List<WriterThread> writerThreads = Lists.newArrayList();

  /* Set of regions which we've decided should not output edits */
  protected final Set<byte[]> blacklistedRegions =
      Collections.synchronizedSet(new TreeSet<>(Bytes.BYTES_COMPARATOR));

  protected boolean closeAndCleanCompleted = false;

  protected boolean writersClosed = false;

  protected final int numThreads;

  protected CancelableProgressable reporter = null;

  protected AtomicLong skippedEdits = new AtomicLong();

  protected List<Path> splits = null;

  public OutputSink(WALSplitter.PipelineController controller, EntryBuffers entryBuffers,
      int numWriters) {
    numThreads = numWriters;
    this.controller = controller;
    this.entryBuffers = entryBuffers;
  }

  void setReporter(CancelableProgressable reporter) {
    this.reporter = reporter;
  }

  /**
   * Start the threads that will pump data from the entryBuffers to the output files.
   */
  public synchronized void startWriterThreads() {
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
   * Update region's maximum edit log SeqNum.
   */
  void updateRegionMaximumEditLogSeqNum(WAL.Entry entry) {
    synchronized (regionMaximumEditLogSeqNum) {
      String regionName = Bytes.toString(entry.getKey().getEncodedRegionName());
      Long currentMaxSeqNum = regionMaximumEditLogSeqNum.get(regionName);
      if (currentMaxSeqNum == null || entry.getKey().getSequenceId() > currentMaxSeqNum) {
        regionMaximumEditLogSeqNum.put(regionName, entry.getKey().getSequenceId());
      }
    }
  }

  /**
   * @return the number of currently opened writers
   */
  int getNumOpenWriters() {
    return this.writers.size();
  }

  long getSkippedEdits() {
    return this.skippedEdits.get();
  }

  /**
   * Wait for writer threads to dump all info to the sink
   * @return true when there is no error
   */
  protected boolean finishWriting(boolean interrupt) throws IOException {
    LOG.debug("Waiting for split writer threads to finish");
    boolean progress_failed = false;
    for (WriterThread t : writerThreads) {
      t.finish();
    }
    if (interrupt) {
      for (WriterThread t : writerThreads) {
        t.interrupt(); // interrupt the writer threads. We are stopping now.
      }
    }

    for (WriterThread t : writerThreads) {
      if (!progress_failed && reporter != null && !reporter.progress()) {
        progress_failed = true;
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
    LOG.info("{} split writers finished; closing.", this.writerThreads.size());
    return (!progress_failed);
  }

  public abstract List<Path> finishWritingAndClose() throws IOException;

  /**
   * @return a map from encoded region ID to the number of edits written out for that region.
   */
  public abstract Map<byte[], Long> getOutputCounts();

  /**
   * @return number of regions we've recovered
   */
  public abstract int getNumberOfRecoveredRegions();

  /**
   * @param buffer A WAL Edit Entry
   */
  public abstract void append(WALSplitter.RegionEntryBuffer buffer) throws IOException;

  /**
   * WriterThread call this function to help flush internal remaining edits in buffer before close
   * @return true when underlying sink has something to flush
   */
  public boolean flush() throws IOException {
    return false;
  }

  /**
   * Some WALEdit's contain only KV's for account on what happened to a region. Not all sinks will
   * want to get all of those edits.
   * @return Return true if this sink wants to accept this region-level WALEdit.
   */
  public abstract boolean keepRegionEvent(WAL.Entry entry);

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
        WALSplitter.RegionEntryBuffer buffer = entryBuffers.getChunkToWrite();
        if (buffer == null) {
          // No data currently available, wait on some more to show up
          synchronized (controller.dataAvailable) {
            if (shouldStop && !this.outputSink.flush()) {
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

    private void writeBuffer(WALSplitter.RegionEntryBuffer buffer) throws IOException {
      outputSink.append(buffer);
    }

    void setShouldStop(boolean shouldStop) {
      this.shouldStop = shouldStop;
    }

    void finish() {
      synchronized (controller.dataAvailable) {
        shouldStop = true;
        controller.dataAvailable.notifyAll();
      }
    }
  }
}
