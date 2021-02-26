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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.MultipleIOException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that manages the output streams from the log splitting process.
 * Every region may have many recovered edits file. But the opening writers is bounded.
 * Bounded means the output streams will be no more than the size of threadpool.
 */
@InterfaceAudience.Private
class BoundedRecoveredEditsOutputSink extends AbstractRecoveredEditsOutputSink {
  private static final Logger LOG =
      LoggerFactory.getLogger(BoundedRecoveredEditsOutputSink.class);

  // Since the splitting process may create multiple output files, we need a map
  // to track the output count of each region.
  private ConcurrentMap<String, Long> regionEditsWrittenMap = new ConcurrentHashMap<>();
  // Need a counter to track the opening writers.
  private final AtomicInteger openingWritersNum = new AtomicInteger(0);

  public BoundedRecoveredEditsOutputSink(WALSplitter walSplitter,
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
    // The key point is create a new writer, write edits then close writer.
    RecoveredEditsWriter writer =
      createRecoveredEditsWriter(buffer.tableName, buffer.encodedRegionName,
        entries.get(0).getKey().getSequenceId());
    if (writer != null) {
      openingWritersNum.incrementAndGet();
      writer.writeRegionEntries(entries);
      regionEditsWrittenMap.compute(Bytes.toString(buffer.encodedRegionName),
        (k, v) -> v == null ? writer.editsWritten : v + writer.editsWritten);
      List<IOException> thrown = new ArrayList<>();
      Path dst = closeRecoveredEditsWriter(writer, thrown);
      splits.add(dst);
      openingWritersNum.decrementAndGet();
      if (!thrown.isEmpty()) {
        throw MultipleIOException.createIOException(thrown);
      }
    }
  }

  @Override
  public List<Path> close() throws IOException {
    boolean isSuccessful = true;
    try {
      isSuccessful = finishWriterThreads(false);
    } finally {
      isSuccessful &= writeRemainingEntryBuffers();
    }
    return isSuccessful ? splits : null;
  }

  /**
   * Write out the remaining RegionEntryBuffers and close the writers.
   *
   * @return true when there is no error.
   */
  private boolean writeRemainingEntryBuffers() throws IOException {
    for (EntryBuffers.RegionEntryBuffer buffer : entryBuffers.buffers.values()) {
      closeCompletionService.submit(() -> {
        append(buffer);
        return null;
      });
    }
    boolean progressFailed = false;
    try {
      for (int i = 0, n = entryBuffers.buffers.size(); i < n; i++) {
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
    return !progressFailed;
  }

  @Override
  public Map<String, Long> getOutputCounts() {
    return regionEditsWrittenMap;
  }

  @Override
  public int getNumberOfRecoveredRegions() {
    return regionEditsWrittenMap.size();
  }

  @Override
  public int getNumOpenWriters() {
    return openingWritersNum.get();
  }
}
