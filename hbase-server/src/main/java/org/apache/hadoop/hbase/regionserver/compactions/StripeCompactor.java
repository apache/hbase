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
import java.util.function.Consumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.ScanInfo;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;
import org.apache.hadoop.hbase.regionserver.StripeMultiFileWriter;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the placeholder for stripe compactor. The implementation, as well as the proper javadoc,
 * will be added in HBASE-7967.
 */
@InterfaceAudience.Private
public class StripeCompactor extends AbstractMultiOutputCompactor<StripeMultiFileWriter> {
  private static final Logger LOG = LoggerFactory.getLogger(StripeCompactor.class);

  public StripeCompactor(Configuration conf, HStore store) {
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
    public ScanType getScanType(CompactionRequestImpl request) {
      // If majorRangeFromRow and majorRangeToRow are not null, then we will not use the return
      // value to create InternalScanner. See the createScanner method below. The return value is
      // also used when calling coprocessor hooks.
      return ScanType.COMPACT_RETAIN_DELETES;
    }

    @Override
    public InternalScanner createScanner(ScanInfo scanInfo, List<StoreFileScanner> scanners,
        ScanType scanType, FileDetails fd, long smallestReadPoint) throws IOException {
      return (majorRangeFromRow == null)
          ? StripeCompactor.this.createScanner(store, scanInfo, scanners, scanType,
            smallestReadPoint, fd.earliestPutTs)
          : StripeCompactor.this.createScanner(store, scanInfo, scanners, smallestReadPoint,
            fd.earliestPutTs, majorRangeFromRow, majorRangeToRow);
    }
  }

  public List<Path> compact(CompactionRequestImpl request, final List<byte[]> targetBoundaries,
      final byte[] majorRangeFromRow, final byte[] majorRangeToRow,
      ThroughputController throughputController, User user) throws IOException {
    if (LOG.isDebugEnabled()) {
      StringBuilder sb = new StringBuilder();
      sb.append("Executing compaction with " + targetBoundaries.size() + " boundaries:");
      for (byte[] tb : targetBoundaries) {
        sb.append(" [").append(Bytes.toString(tb)).append("]");
      }
      LOG.debug(sb.toString());
    }
    return compact(
      request,
      new StripeInternalScannerFactory(majorRangeFromRow, majorRangeToRow),
      new CellSinkFactory<StripeMultiFileWriter>() {

        @Override
        public StripeMultiFileWriter createWriter(InternalScanner scanner, FileDetails fd,
          boolean shouldDropBehind, boolean major, Consumer<Path> writerCreationTracker)
          throws IOException {
          StripeMultiFileWriter writer = new StripeMultiFileWriter.BoundaryMultiWriter(
            store.getComparator(),
            targetBoundaries,
            majorRangeFromRow,
            majorRangeToRow);
          initMultiWriter(writer, scanner, fd, shouldDropBehind, major, writerCreationTracker);
          return writer;
        }
      },
      throughputController,
      user);
  }

  public List<Path> compact(CompactionRequestImpl request, final int targetCount, final long targetSize,
      final byte[] left, final byte[] right, byte[] majorRangeFromRow, byte[] majorRangeToRow,
      ThroughputController throughputController, User user) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug(
        "Executing compaction with " + targetSize + " target file size, no more than " + targetCount
            + " files, in [" + Bytes.toString(left) + "] [" + Bytes.toString(right) + "] range");
    }
    return compact(request, new StripeInternalScannerFactory(majorRangeFromRow, majorRangeToRow),
      new CellSinkFactory<StripeMultiFileWriter>() {

        @Override
        public StripeMultiFileWriter createWriter(InternalScanner scanner, FileDetails fd,
          boolean shouldDropBehind, boolean major, Consumer<Path> writerCreationTracker)
          throws IOException {
          StripeMultiFileWriter writer = new StripeMultiFileWriter.SizeMultiWriter(
            store.getComparator(),
            targetCount,
            targetSize,
            left,
            right);
          initMultiWriter(writer, scanner, fd, shouldDropBehind, major, writerCreationTracker);
          return writer;
        }
      },
      throughputController,
      user);
  }

  @Override
  protected List<Path> commitWriter(StripeMultiFileWriter writer, FileDetails fd,
      CompactionRequestImpl request) throws IOException {
    List<Path> newFiles = writer.commitWriters(fd.maxSeqId, request.isMajor(), request.getFiles());
    assert !newFiles.isEmpty() : "Should have produced an empty file to preserve metadata.";
    return newFiles;
  }

}
