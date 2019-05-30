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
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALSplitter.PipelineController;
import org.apache.hadoop.hbase.wal.WALSplitter.RegionEntryBuffer;
import org.apache.yetus.audience.InterfaceAudience;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class which accumulates edits and separates them into a buffer per region while simultaneously
 * accounting RAM usage. Blocks if the RAM usage crosses a predefined threshold. Writer threads then
 * pull region-specific buffers from this class.
 */
@InterfaceAudience.Private
public class EntryBuffers {
  private static final Logger LOG = LoggerFactory.getLogger(EntryBuffers.class);

  PipelineController controller;

  Map<byte[], RegionEntryBuffer> buffers = new TreeMap<>(Bytes.BYTES_COMPARATOR);

  /*
   * Track which regions are currently in the middle of writing. We don't allow an IO thread to pick
   * up bytes from a region if we're already writing data for that region in a different IO thread.
   */
  Set<byte[]> currentlyWriting = new TreeSet<>(Bytes.BYTES_COMPARATOR);

  long totalBuffered = 0;
  long maxHeapUsage;
  boolean splitWriterCreationBounded;

  public EntryBuffers(PipelineController controller, long maxHeapUsage) {
    this(controller, maxHeapUsage, false);
  }

  public EntryBuffers(PipelineController controller, long maxHeapUsage,
      boolean splitWriterCreationBounded) {
    this.controller = controller;
    this.maxHeapUsage = maxHeapUsage;
    this.splitWriterCreationBounded = splitWriterCreationBounded;
  }

  /**
   * Append a log entry into the corresponding region buffer. Blocks if the total heap usage has
   * crossed the specified threshold.
   */
  public void appendEntry(WAL.Entry entry) throws InterruptedException, IOException {
    WALKey key = entry.getKey();
    RegionEntryBuffer buffer;
    long incrHeap;
    synchronized (this) {
      buffer = buffers.get(key.getEncodedRegionName());
      if (buffer == null) {
        buffer = new RegionEntryBuffer(key.getTableName(), key.getEncodedRegionName());
        buffers.put(key.getEncodedRegionName(), buffer);
      }
      incrHeap = buffer.appendEntry(entry);
    }

    // If we crossed the chunk threshold, wait for more space to be available
    synchronized (controller.dataAvailable) {
      totalBuffered += incrHeap;
      while (totalBuffered > maxHeapUsage && controller.thrown.get() == null) {
        LOG.debug("Used {} bytes of buffered edits, waiting for IO threads", totalBuffered);
        controller.dataAvailable.wait(2000);
      }
      controller.dataAvailable.notifyAll();
    }
    controller.checkForErrors();
  }

  /**
   * @return RegionEntryBuffer a buffer of edits to be written.
   */
  synchronized RegionEntryBuffer getChunkToWrite() {
    // The core part of limiting opening writers is it doesn't return chunk only if the
    // heap size is over maxHeapUsage. Thus it doesn't need to create a writer for each
    // region during splitting. It will flush all the logs in the buffer after splitting
    // through a threadpool, which means the number of writers it created is under control.
    if (splitWriterCreationBounded && totalBuffered < maxHeapUsage) {
      return null;
    }
    long biggestSize = 0;
    byte[] biggestBufferKey = null;

    for (Map.Entry<byte[], RegionEntryBuffer> entry : buffers.entrySet()) {
      long size = entry.getValue().heapSize();
      if (size > biggestSize && (!currentlyWriting.contains(entry.getKey()))) {
        biggestSize = size;
        biggestBufferKey = entry.getKey();
      }
    }
    if (biggestBufferKey == null) {
      return null;
    }

    RegionEntryBuffer buffer = buffers.remove(biggestBufferKey);
    currentlyWriting.add(biggestBufferKey);
    return buffer;
  }

  void doneWriting(RegionEntryBuffer buffer) {
    synchronized (this) {
      boolean removed = currentlyWriting.remove(buffer.encodedRegionName);
      assert removed;
    }
    long size = buffer.heapSize();

    synchronized (controller.dataAvailable) {
      totalBuffered -= size;
      // We may unblock writers
      controller.dataAvailable.notifyAll();
    }
  }

  synchronized boolean isRegionCurrentlyWriting(byte[] region) {
    return currentlyWriting.contains(region);
  }

  public void waitUntilDrained() {
    synchronized (controller.dataAvailable) {
      while (totalBuffered > 0) {
        try {
          controller.dataAvailable.wait(2000);
        } catch (InterruptedException e) {
          LOG.warn("Got interrupted while waiting for EntryBuffers is drained");
          Thread.interrupted();
          break;
        }
      }
    }
  }
}