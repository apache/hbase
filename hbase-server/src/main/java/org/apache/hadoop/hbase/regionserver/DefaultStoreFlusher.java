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

import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;
import org.apache.hadoop.util.StringUtils;

/**
 * Default implementation of StoreFlusher.
 */
@InterfaceAudience.Private
public class DefaultStoreFlusher extends StoreFlusher {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultStoreFlusher.class);
  private final Object flushLock = new Object();

  public DefaultStoreFlusher(Configuration conf, HStore store) {
    super(conf, store);
  }

  @Override
  public List<Path> flushSnapshot(MemStoreSnapshot snapshot, long cacheFlushId,
      MonitoredTask status, ThroughputController throughputController,
      FlushLifeCycleTracker tracker) throws IOException {
    ArrayList<Path> result = new ArrayList<>();
    int cellsCount = snapshot.getCellsCount();
    if (cellsCount == 0) return result; // don't flush if there are no entries

    // Use a store scanner to find which rows to flush.
    InternalScanner scanner = createScanner(snapshot.getScanners(), tracker);
    StoreFileWriter writer;
    try {
      // TODO:  We can fail in the below block before we complete adding this flush to
      //        list of store files.  Add cleanup of anything put on filesystem if we fail.
      synchronized (flushLock) {
        status.setStatus("Flushing " + store + ": creating writer");
        // Write the map out to the disk
        writer = store.createWriterInTmp(cellsCount,
            store.getColumnFamilyDescriptor().getCompressionType(), false, true,
            snapshot.isTagsPresent(), false);
        IOException e = null;
        try {
          performFlush(scanner, writer, throughputController);
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
    LOG.info("Flushed memstore data size={} at sequenceid={} (bloomFilter={}), to={}",
        StringUtils.byteDesc(snapshot.getDataSize()), cacheFlushId, writer.hasGeneralBloom(),
        writer.getPath());
    result.add(writer.getPath());
    return result;
  }
}
