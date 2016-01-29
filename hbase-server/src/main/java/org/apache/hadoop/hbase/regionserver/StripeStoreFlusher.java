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

import static org.apache.hadoop.hbase.regionserver.StripeStoreFileManager.OPEN_KEY;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.regionserver.StoreFile.Writer;
import org.apache.hadoop.hbase.regionserver.StripeMultiFileWriter;
import org.apache.hadoop.hbase.regionserver.compactions.StripeCompactionPolicy;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;

import com.google.common.annotations.VisibleForTesting;

/**
 * Stripe implementation of StoreFlusher. Flushes files either into L0 file w/o metadata, or
 * into separate striped files, avoiding L0.
 */
@InterfaceAudience.Private
public class StripeStoreFlusher extends StoreFlusher {
  private static final Log LOG = LogFactory.getLog(StripeStoreFlusher.class);
  private final Object flushLock = new Object();
  private final StripeCompactionPolicy policy;
  private final StripeCompactionPolicy.StripeInformationProvider stripes;

  public StripeStoreFlusher(Configuration conf, Store store,
      StripeCompactionPolicy policy, StripeStoreFileManager stripes) {
    super(conf, store);
    this.policy = policy;
    this.stripes = stripes;
  }

  @Override
  public List<Path> flushSnapshot(MemStoreSnapshot snapshot, long cacheFlushSeqNum,
      MonitoredTask status, ThroughputController throughputController) throws IOException {
    List<Path> result = new ArrayList<Path>();
    int cellsCount = snapshot.getCellsCount();
    if (cellsCount == 0) return result; // don't flush if there are no entries

    long smallestReadPoint = store.getSmallestReadPoint();
    InternalScanner scanner = createScanner(snapshot.getScanner(), smallestReadPoint);
    if (scanner == null) {
      return result; // NULL scanner returned from coprocessor hooks means skip normal processing
    }

    // Let policy select flush method.
    StripeFlushRequest req = this.policy.selectFlush(this.stripes, cellsCount);

    boolean success = false;
    StripeMultiFileWriter mw = null;
    try {
      mw = req.createWriter(); // Writer according to the policy.
      StripeMultiFileWriter.WriterFactory factory = createWriterFactory(
          snapshot.getTimeRangeTracker(), cellsCount);
      StoreScanner storeScanner = (scanner instanceof StoreScanner) ? (StoreScanner)scanner : null;
      mw.init(storeScanner, factory, store.getComparator());

      synchronized (flushLock) {
        performFlush(scanner, mw, smallestReadPoint, throughputController);
        result = mw.commitWriters(cacheFlushSeqNum, false);
        success = true;
      }
    } finally {
      if (!success && (mw != null)) {
        for (Path leftoverFile : mw.abortWriters()) {
          try {
            store.getFileSystem().delete(leftoverFile, false);
          } catch (Exception e) {
            LOG.error("Failed to delete a file after failed flush: " + e);
          }
        }
      }
      try {
        scanner.close();
      } catch (IOException ex) {
        LOG.warn("Failed to close flush scanner, ignoring", ex);
      }
    }
    return result;
  }

  private StripeMultiFileWriter.WriterFactory createWriterFactory(
      final TimeRangeTracker tracker, final long kvCount) {
    return new StripeMultiFileWriter.WriterFactory() {
      @Override
      public Writer createWriter() throws IOException {
        StoreFile.Writer writer = store.createWriterInTmp(
            kvCount, store.getFamily().getCompression(),
            /* isCompaction = */ false,
            /* includeMVCCReadpoint = */ true,
            /* includesTags = */ true,
            /* shouldDropBehind = */ false);
        writer.setTimeRangeTracker(tracker);
        return writer;
      }
    };
  }

  /** Stripe flush request wrapper that writes a non-striped file. */
  public static class StripeFlushRequest {
    @VisibleForTesting
    public StripeMultiFileWriter createWriter() throws IOException {
      StripeMultiFileWriter writer =
          new StripeMultiFileWriter.SizeMultiWriter(1, Long.MAX_VALUE, OPEN_KEY, OPEN_KEY);
      writer.setNoStripeMetadata();
      return writer;
    }
  }

  /** Stripe flush request wrapper based on boundaries. */
  public static class BoundaryStripeFlushRequest extends StripeFlushRequest {
    private final List<byte[]> targetBoundaries;

    /** @param targetBoundaries New files should be written with these boundaries. */
    public BoundaryStripeFlushRequest(List<byte[]> targetBoundaries) {
      this.targetBoundaries = targetBoundaries;
    }

    @Override
    public StripeMultiFileWriter createWriter() throws IOException {
      return new StripeMultiFileWriter.BoundaryMultiWriter(targetBoundaries, null, null);
    }
  }

  /** Stripe flush request wrapper based on size. */
  public static class SizeStripeFlushRequest extends StripeFlushRequest {
    private final int targetCount;
    private final long targetKvs;

    /**
     * @param targetCount The maximum number of stripes to flush into.
     * @param targetKvs The KV count of each segment. If targetKvs*targetCount is less than
     *                  total number of kvs, all the overflow data goes into the last stripe.
     */
    public SizeStripeFlushRequest(int targetCount, long targetKvs) {
      this.targetCount = targetCount;
      this.targetKvs = targetKvs;
    }

    @Override
    public StripeMultiFileWriter createWriter() throws IOException {
      return new StripeMultiFileWriter.SizeMultiWriter(
          this.targetCount, this.targetKvs, OPEN_KEY, OPEN_KEY);
    }
  }
}
