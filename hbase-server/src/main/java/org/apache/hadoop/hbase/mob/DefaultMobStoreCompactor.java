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
package org.apache.hadoop.hbase.mob;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.CellSink;
import org.apache.hadoop.hbase.regionserver.HMobStore;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.ScanInfo;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.regionserver.ShipperListener;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;
import org.apache.hadoop.hbase.regionserver.StoreFileWriter;
import org.apache.hadoop.hbase.regionserver.StoreScanner;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequestImpl;
import org.apache.hadoop.hbase.regionserver.compactions.DefaultCompactor;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputControlUtil;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Compact passed set of files in the mob-enabled column family.
 */
@InterfaceAudience.Private
public class DefaultMobStoreCompactor extends DefaultCompactor {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultMobStoreCompactor.class);
  private long mobSizeThreshold;
  private HMobStore mobStore;

  private final InternalScannerFactory scannerFactory = new InternalScannerFactory() {

    @Override
    public ScanType getScanType(CompactionRequestImpl request) {
      // retain the delete markers until they are expired.
      return ScanType.COMPACT_RETAIN_DELETES;
    }

    @Override
    public InternalScanner createScanner(ScanInfo scanInfo, List<StoreFileScanner> scanners,
        ScanType scanType, FileDetails fd, long smallestReadPoint) throws IOException {
      return new StoreScanner(store, scanInfo, scanners, scanType, smallestReadPoint,
          fd.earliestPutTs);
    }
  };

  private final CellSinkFactory<StoreFileWriter> writerFactory =
      new CellSinkFactory<StoreFileWriter>() {
        @Override
        public StoreFileWriter createWriter(InternalScanner scanner,
            org.apache.hadoop.hbase.regionserver.compactions.Compactor.FileDetails fd,
            boolean shouldDropBehind) throws IOException {
          // make this writer with tags always because of possible new cells with tags.
          return store.createWriterInTmp(fd.maxKeyCount, compactionCompression, true, true, true,
            shouldDropBehind);
        }
      };

  public DefaultMobStoreCompactor(Configuration conf, HStore store) {
    super(conf, store);
    // The mob cells reside in the mob-enabled column family which is held by HMobStore.
    // During the compaction, the compactor reads the cells from the mob files and
    // probably creates new mob files. All of these operations are included in HMobStore,
    // so we need to cast the Store to HMobStore.
    if (!(store instanceof HMobStore)) {
      throw new IllegalArgumentException("The store " + store + " is not a HMobStore");
    }
    mobStore = (HMobStore) store;
    mobSizeThreshold = store.getColumnFamilyDescriptor().getMobThreshold();
  }

  @Override
  public List<Path> compact(CompactionRequestImpl request, ThroughputController throughputController,
      User user) throws IOException {
    return compact(request, scannerFactory, writerFactory, throughputController, user);
  }

  /**
   * Performs compaction on a column family with the mob flag enabled.
   * This is for when the mob threshold size has changed or if the mob
   * column family mode has been toggled via an alter table statement.
   * Compacts the files by the following rules.
   * 1. If the Put cell has a mob reference tag, the cell's value is the path of the mob file.
   * <ol>
   * <li>
   * If the value size of a cell is larger than the threshold, this cell is regarded as a mob,
   * directly copy the (with mob tag) cell into the new store file.
   * </li>
   * <li>
   * Otherwise, retrieve the mob cell from the mob file, and writes a copy of the cell into
   * the new store file.
   * </li>
   * </ol>
   * 2. If the Put cell doesn't have a reference tag.
   * <ol>
   * <li>
   * If the value size of a cell is larger than the threshold, this cell is regarded as a mob,
   * write this cell to a mob file, and write the path of this mob file to the store file.
   * </li>
   * <li>
   * Otherwise, directly write this cell into the store file.
   * </li>
   * </ol>
   * 3. Decide how to write a Delete cell.
   * <ol>
   * <li>
   * If a Delete cell does not have a mob reference tag which means this delete marker have not
   * been written to the mob del file, write this cell to the mob del file, and write this cell
   * with a ref tag to a store file.
   * </li>
   * <li>
   * Otherwise, directly write it to a store file.
   * </li>
   * </ol>
   * After the major compaction on the normal hfiles, we have a guarantee that we have purged all
   * deleted or old version mob refs, and the delete markers are written to a del file with the
   * suffix _del. Because of this, it is safe to use the del file in the mob compaction.
   * The mob compaction doesn't take place in the normal hfiles, it occurs directly in the
   * mob files. When the small mob files are merged into bigger ones, the del file is added into
   * the scanner to filter the deleted cells.
   * @param fd File details
   * @param scanner Where to read from.
   * @param writer Where to write to.
   * @param smallestReadPoint Smallest read point.
   * @param cleanSeqId When true, remove seqId(used to be mvcc) value which is <= smallestReadPoint
   * @param throughputController The compaction throughput controller.
   * @param major Is a major compaction.
   * @param numofFilesToCompact the number of files to compact
   * @return Whether compaction ended; false if it was interrupted for any reason.
   */
  @Override
  protected boolean performCompaction(FileDetails fd, InternalScanner scanner, CellSink writer,
      long smallestReadPoint, boolean cleanSeqId, ThroughputController throughputController,
      boolean major, int numofFilesToCompact) throws IOException {
    long bytesWrittenProgressForCloseCheck = 0;
    long bytesWrittenProgressForLog = 0;
    long bytesWrittenProgressForShippedCall = 0;
    // Since scanner.next() can return 'false' but still be delivering data,
    // we have to use a do/while loop.
    List<Cell> cells = new ArrayList<>();
    // Limit to "hbase.hstore.compaction.kv.max" (default 10) to avoid OOME
    int closeCheckSizeLimit = HStore.getCloseCheckInterval();
    long lastMillis = 0;
    if (LOG.isDebugEnabled()) {
      lastMillis = EnvironmentEdgeManager.currentTime();
    }
    String compactionName = ThroughputControlUtil.getNameForThrottling(store, "compaction");
    long now = 0;
    boolean hasMore;
    Path path = MobUtils.getMobFamilyPath(conf, store.getTableName(), store.getColumnFamilyName());
    byte[] fileName = null;
    StoreFileWriter mobFileWriter = null, delFileWriter = null;
    long mobCells = 0, deleteMarkersCount = 0;
    long cellsCountCompactedToMob = 0, cellsCountCompactedFromMob = 0;
    long cellsSizeCompactedToMob = 0, cellsSizeCompactedFromMob = 0;
    boolean finished = false;
    ScannerContext scannerContext =
        ScannerContext.newBuilder().setBatchLimit(compactionKVMax).build();
    throughputController.start(compactionName);
    KeyValueScanner kvs = (scanner instanceof KeyValueScanner)? (KeyValueScanner)scanner : null;
    long shippedCallSizeLimit = (long) numofFilesToCompact * this.store.getColumnFamilyDescriptor().getBlocksize();
    try {
      try {
        // If the mob file writer could not be created, directly write the cell to the store file.
        mobFileWriter = mobStore.createWriterInTmp(new Date(fd.latestPutTs), fd.maxKeyCount,
          compactionCompression, store.getRegionInfo().getStartKey(), true);
        fileName = Bytes.toBytes(mobFileWriter.getPath().getName());
      } catch (IOException e) {
        LOG.warn("Failed to create mob writer, "
               + "we will continue the compaction by writing MOB cells directly in store files", e);
      }
      if (major) {
        try {
          delFileWriter = mobStore.createDelFileWriterInTmp(new Date(fd.latestPutTs),
            fd.maxKeyCount, compactionCompression, store.getRegionInfo().getStartKey());
        } catch (IOException e) {
          LOG.warn(
            "Failed to create del writer, "
            + "we will continue the compaction by writing delete markers directly in store files",
            e);
        }
      }
      do {
        hasMore = scanner.next(cells, scannerContext);
        if (LOG.isDebugEnabled()) {
          now = EnvironmentEdgeManager.currentTime();
        }
        for (Cell c : cells) {
          if (major && CellUtil.isDelete(c)) {
            if (MobUtils.isMobReferenceCell(c) || delFileWriter == null) {
              // Directly write it to a store file
              writer.append(c);
            } else {
              // Add a ref tag to this cell and write it to a store file.
              writer.append(MobUtils.createMobRefDeleteMarker(c));
              // Write the cell to a del file
              delFileWriter.append(c);
              deleteMarkersCount++;
            }
          } else if (mobFileWriter == null || c.getTypeByte() != KeyValue.Type.Put.getCode()) {
            // If the mob file writer is null or the kv type is not put, directly write the cell
            // to the store file.
            writer.append(c);
          } else if (MobUtils.isMobReferenceCell(c)) {
            if (MobUtils.hasValidMobRefCellValue(c)) {
              int size = MobUtils.getMobValueLength(c);
              if (size > mobSizeThreshold) {
                // If the value size is larger than the threshold, it's regarded as a mob. Since
                // its value is already in the mob file, directly write this cell to the store file
                writer.append(c);
              } else {
                // If the value is not larger than the threshold, it's not regarded a mob. Retrieve
                // the mob cell from the mob file, and write it back to the store file. Must
                // close the mob scanner once the life cycle finished.
                try (MobCell mobCell = mobStore.resolve(c, false)) {
                  if (mobCell.getCell().getValueLength() != 0) {
                    // put the mob data back to the store file
                    PrivateCellUtil.setSequenceId(mobCell.getCell(), c.getSequenceId());
                    writer.append(mobCell.getCell());
                    cellsCountCompactedFromMob++;
                    cellsSizeCompactedFromMob += mobCell.getCell().getValueLength();
                  } else {
                    // If the value of a file is empty, there might be issues when retrieving,
                    // directly write the cell to the store file, and leave it to be handled by the
                    // next compaction.
                    writer.append(c);
                  }
                }
              }
            } else {
              LOG.warn("The value format of the KeyValue " + c
                  + " is wrong, its length is less than " + Bytes.SIZEOF_INT);
              writer.append(c);
            }
          } else if (c.getValueLength() <= mobSizeThreshold) {
            //If value size of a cell is not larger than the threshold, directly write to store file
            writer.append(c);
          } else {
            // If the value size of a cell is larger than the threshold, it's regarded as a mob,
            // write this cell to a mob file, and write the path to the store file.
            mobCells++;
            // append the original keyValue in the mob file.
            mobFileWriter.append(c);
            Cell reference = MobUtils.createMobRefCell(c, fileName,
                this.mobStore.getRefCellTags());
            // write the cell whose value is the path of a mob file to the store file.
            writer.append(reference);
            cellsCountCompactedToMob++;
            cellsSizeCompactedToMob += c.getValueLength();
          }
          int len = c.getSerializedSize();
          ++progress.currentCompactedKVs;
          progress.totalCompactedSize += len;
          bytesWrittenProgressForShippedCall += len;
          if (LOG.isDebugEnabled()) {
            bytesWrittenProgressForLog += len;
          }
          throughputController.control(compactionName, len);
          // check periodically to see if a system stop is requested
          if (closeCheckSizeLimit > 0) {
            bytesWrittenProgressForCloseCheck += len;
            if (bytesWrittenProgressForCloseCheck > closeCheckSizeLimit) {
              bytesWrittenProgressForCloseCheck = 0;
              if (!store.areWritesEnabled()) {
                progress.cancel();
                return false;
              }
            }
          }
          if (kvs != null && bytesWrittenProgressForShippedCall > shippedCallSizeLimit) {
            ((ShipperListener)writer).beforeShipped();
            kvs.shipped();
            bytesWrittenProgressForShippedCall = 0;
          }
        }
        // Log the progress of long running compactions every minute if
        // logging at DEBUG level
        if (LOG.isDebugEnabled()) {
          if ((now - lastMillis) >= COMPACTION_PROGRESS_LOG_INTERVAL) {
            String rate = String.format("%.2f",
              (bytesWrittenProgressForLog / 1024.0) / ((now - lastMillis) / 1000.0));
            LOG.debug("Compaction progress: {} {}, rate={} KB/sec, throughputController is {}",
              compactionName, progress, rate, throughputController);
            lastMillis = now;
            bytesWrittenProgressForLog = 0;
          }
        }
        cells.clear();
      } while (hasMore);
      finished = true;
    } catch (InterruptedException e) {
      progress.cancel();
      throw new InterruptedIOException(
          "Interrupted while control throughput of compacting " + compactionName);
    } finally {
      // Clone last cell in the final because writer will append last cell when committing. If
      // don't clone here and once the scanner get closed, then the memory of last cell will be
      // released. (HBASE-22582)
      ((ShipperListener) writer).beforeShipped();
      throughputController.finish(compactionName);
      if (!finished && mobFileWriter != null) {
        abortWriter(mobFileWriter);
      }
      if (!finished && delFileWriter != null) {
        abortWriter(delFileWriter);
      }
    }
    if (delFileWriter != null) {
      if (deleteMarkersCount > 0) {
        // If the del file is not empty, commit it.
        // If the commit fails, the compaction is re-performed again.
        delFileWriter.appendMetadata(fd.maxSeqId, major, deleteMarkersCount);
        delFileWriter.close();
        mobStore.commitFile(delFileWriter.getPath(), path);
      } else {
        // If the del file is empty, delete it instead of committing.
        abortWriter(delFileWriter);
      }
    }
    if (mobFileWriter != null) {
      if (mobCells > 0) {
        // If the mob file is not empty, commit it.
        mobFileWriter.appendMetadata(fd.maxSeqId, major, mobCells);
        mobFileWriter.close();
        mobStore.commitFile(mobFileWriter.getPath(), path);
      } else {
        // If the mob file is empty, delete it instead of committing.
        abortWriter(mobFileWriter);
      }
    }
    mobStore.updateCellsCountCompactedFromMob(cellsCountCompactedFromMob);
    mobStore.updateCellsCountCompactedToMob(cellsCountCompactedToMob);
    mobStore.updateCellsSizeCompactedFromMob(cellsSizeCompactedFromMob);
    mobStore.updateCellsSizeCompactedToMob(cellsSizeCompactedToMob);
    progress.complete();
    return true;
  }
}
