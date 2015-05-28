/**
 *
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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.util.StringUtils;

/**
 * Default implementation of StoreFlusher.
 */
@InterfaceAudience.Private
public class DefaultStoreFlusher extends StoreFlusher {
  private static final Log LOG = LogFactory.getLog(DefaultStoreFlusher.class);
  private final Object flushLock = new Object();

  public DefaultStoreFlusher(Configuration conf, Store store) {
    super(conf, store);
  }

  @Override
  public List<Path> flushSnapshot(MemStoreSnapshot snapshot, long cacheFlushId,
      MonitoredTask status) throws IOException {
    ArrayList<Path> result = new ArrayList<Path>();
    int cellsCount = snapshot.getCellsCount();
    if (cellsCount == 0) return result; // don't flush if there are no entries

    // Use a store scanner to find which rows to flush.
    long smallestReadPoint = store.getSmallestReadPoint();
    InternalScanner scanner = createScanner(snapshot.getScanner(), smallestReadPoint);
    if (scanner == null) {
      return result; // NULL scanner returned from coprocessor hooks means skip normal processing
    }

    StoreFile.Writer writer;
    try {
      // TODO:  We can fail in the below block before we complete adding this flush to
      //        list of store files.  Add cleanup of anything put on filesystem if we fail.
      synchronized (flushLock) {
        status.setStatus("Flushing " + store + ": creating writer");
        // Write the map out to the disk
        writer = store.createWriterInTmp(
            cellsCount, store.getFamily().getCompression(), false, true, snapshot.isTagsPresent());
        writer.setTimeRangeTracker(snapshot.getTimeRangeTracker());
        IOException e = null;
        try {
          performFlush(scanner, writer, smallestReadPoint);
        } catch (IOException ioe) {
          e = ioe;
          // throw the exception out
          throw ioe;
        } finally {
          if (e != null) {
            writer.close();
          } else {
            finalizeWriter(writer, cacheFlushId, status);
          }
        }
      }
    } finally {
      scanner.close();
    }
    LOG.info("Flushed, sequenceid=" + cacheFlushId +", memsize="
        + StringUtils.humanReadableInt(snapshot.getSize()) +
        ", hasBloomFilter=" + writer.hasGeneralBloom() +
        ", into tmp file " + writer.getPath());
    result.add(writer.getPath());
    return result;
  }
}
