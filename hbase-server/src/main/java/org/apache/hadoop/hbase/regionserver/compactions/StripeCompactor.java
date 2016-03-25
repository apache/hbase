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
package org.apache.hadoop.hbase.regionserver.compactions;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;
import org.apache.hadoop.hbase.regionserver.StripeMultiFileWriter;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * This is the placeholder for stripe compactor. The implementation, as well as the proper javadoc,
 * will be added in HBASE-7967.
 */
@InterfaceAudience.Private
public class StripeCompactor extends AbstractMultiOutputCompactor<StripeMultiFileWriter> {
  private static final Log LOG = LogFactory.getLog(StripeCompactor.class);

  public StripeCompactor(Configuration conf, Store store) {
    super(conf, store);
  }

  private final class StripeInternalScannerFactory implements InternalScannerFactory {

    private final byte[] majorRangeFromRow;

    private final byte[] majorRangeToRow;

    public StripeInternalScannerFactory(byte[] majorRangeFromRow, byte[] majorRangeToRow) {
      this.majorRangeFromRow = majorRangeFromRow;
      this.majorRangeToRow = majorRangeToRow;
    }

    @Override
    public ScanType getScanType(CompactionRequest request) {
      // If majorRangeFromRow and majorRangeToRow are not null, then we will not use the return
      // value to create InternalScanner. See the createScanner method below. The return value is
      // also used when calling coprocessor hooks.
      return ScanType.COMPACT_RETAIN_DELETES;
    }

    @Override
    public InternalScanner createScanner(List<StoreFileScanner> scanners, ScanType scanType,
        FileDetails fd, long smallestReadPoint) throws IOException {
      return (majorRangeFromRow == null) ? StripeCompactor.this.createScanner(store, scanners,
        scanType, smallestReadPoint, fd.earliestPutTs) : StripeCompactor.this.createScanner(store,
        scanners, smallestReadPoint, fd.earliestPutTs, majorRangeFromRow, majorRangeToRow);
    }
  }

  public List<Path> compact(CompactionRequest request, List<byte[]> targetBoundaries,
      byte[] majorRangeFromRow, byte[] majorRangeToRow, ThroughputController throughputController,
      User user) throws IOException {
    if (LOG.isDebugEnabled()) {
      StringBuilder sb = new StringBuilder();
      sb.append("Executing compaction with " + targetBoundaries.size() + " boundaries:");
      for (byte[] tb : targetBoundaries) {
        sb.append(" [").append(Bytes.toString(tb)).append("]");
      }
      LOG.debug(sb.toString());
    }
    StripeMultiFileWriter writer =
        new StripeMultiFileWriter.BoundaryMultiWriter(store.getComparator(), targetBoundaries,
            majorRangeFromRow, majorRangeToRow);
    return compact(writer, request, new StripeInternalScannerFactory(majorRangeFromRow,
        majorRangeToRow), throughputController, user);
  }

  public List<Path> compact(CompactionRequest request, int targetCount, long targetSize,
      byte[] left, byte[] right, byte[] majorRangeFromRow, byte[] majorRangeToRow,
      ThroughputController throughputController, User user) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Executing compaction with " + targetSize + " target file size, no more than "
          + targetCount + " files, in [" + Bytes.toString(left) + "] [" + Bytes.toString(right)
          + "] range");
    }
    StripeMultiFileWriter writer =
        new StripeMultiFileWriter.SizeMultiWriter(store.getComparator(), targetCount, targetSize,
            left, right);
    return compact(writer, request, new StripeInternalScannerFactory(majorRangeFromRow,
        majorRangeToRow), throughputController, user);
  }

  @Override
  protected List<Path> commitMultiWriter(StripeMultiFileWriter writer, FileDetails fd,
      CompactionRequest request) throws IOException {
    List<Path> newFiles = writer.commitWriters(fd.maxSeqId, request.isMajor());
    assert !newFiles.isEmpty() : "Should have produced an empty file to preserve metadata.";
    return newFiles;
  }
}
