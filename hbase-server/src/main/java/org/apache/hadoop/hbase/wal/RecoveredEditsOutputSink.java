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
package org.apache.hadoop.hbase.wal;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.MultipleIOException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

/**
 * Class that manages the output streams from the log splitting process.
 * Every region only has one recovered edits file PER split WAL (if we split
 * multiple WALs during a log-splitting session, on open, a Region may
 * have multiple recovered.edits files to replay -- one per split WAL).
 * @see BoundedRecoveredEditsOutputSink which is like this class but imposes upper bound on
 *   the number of writers active at one time (makes for better throughput).
 */
@InterfaceAudience.Private
class RecoveredEditsOutputSink extends AbstractRecoveredEditsOutputSink {
  private static final Logger LOG = LoggerFactory.getLogger(RecoveredEditsOutputSink.class);
  private ConcurrentMap<String, RecoveredEditsWriter> writers = new ConcurrentHashMap<>();

  public RecoveredEditsOutputSink(WALSplitter walSplitter,
      WALSplitter.PipelineController controller, EntryBuffers entryBuffers, int numWriters) {
    super(walSplitter, controller, entryBuffers, numWriters);
  }

  @Override
  public void append(EntryBuffers.RegionEntryBuffer buffer) throws IOException {
    List<WAL.Entry> entries = buffer.entries;
    if (entries.isEmpty()) {
      LOG.warn("got an empty buffer, skipping");
      return;
    }
    RecoveredEditsWriter writer =
      getRecoveredEditsWriter(buffer.tableName, buffer.encodedRegionName,
        entries.get(0).getKey().getSequenceId());
    if (writer != null) {
      writer.writeRegionEntries(entries);
    }
  }

  /**
   * Get a writer and path for a log starting at the given entry. This function is threadsafe so
   * long as multiple threads are always acting on different regions.
   * @return null if this region shouldn't output any logs
   */
  private RecoveredEditsWriter getRecoveredEditsWriter(TableName tableName, byte[] region,
      long seqId) throws IOException {
    RecoveredEditsWriter ret = writers.get(Bytes.toString(region));
    if (ret != null) {
      return ret;
    }
    ret = createRecoveredEditsWriter(tableName, region, seqId);
    if (ret == null) {
      return null;
    }
    LOG.trace("Created {}", ret.path);
    writers.put(Bytes.toString(region), ret);
    return ret;
  }

  @Override
  public List<Path> close() throws IOException {
    boolean isSuccessful = true;
    try {
      isSuccessful = finishWriterThreads(false);
    } finally {
      isSuccessful &= closeWriters();
    }
    return isSuccessful ? splits : null;
  }

  /**
   * Close all of the output streams.
   *
   * @return true when there is no error.
   */
  private boolean closeWriters() throws IOException {
    List<IOException> thrown = Lists.newArrayList();
    for (RecoveredEditsWriter writer : writers.values()) {
      closeCompletionService.submit(() -> {
        Path dst = closeRecoveredEditsWriter(writer, thrown);
        LOG.trace("Closed {}", dst);
        splits.add(dst);
        return null;
      });
    }
    boolean progressFailed = false;
    try {
      for (int i = 0, n = this.writers.size(); i < n; i++) {
        Future<Void> future = closeCompletionService.take();
        future.get();
        if (!progressFailed && reporter != null && !reporter.progress()) {
          progressFailed = true;
        }
      }
    } catch (InterruptedException e) {
      IOException iie = new InterruptedIOException();
      iie.initCause(e);
      throw iie;
    } catch (ExecutionException e) {
      throw new IOException(e.getCause());
    } finally {
      closeThreadPool.shutdownNow();
    }
    if (!thrown.isEmpty()) {
      throw MultipleIOException.createIOException(thrown);
    }
    return !progressFailed;
  }

  @Override
  public Map<String, Long> getOutputCounts() {
    TreeMap<String, Long> ret = new TreeMap<>();
    for (Map.Entry<String, RecoveredEditsWriter> entry : writers.entrySet()) {
      ret.put(entry.getKey(), entry.getValue().editsWritten);
    }
    return ret;
  }

  @Override
  public int getNumberOfRecoveredRegions() {
    return writers.size();
  }

  @Override
  public int getNumOpenWriters() {
    return writers.size();
  }
}
