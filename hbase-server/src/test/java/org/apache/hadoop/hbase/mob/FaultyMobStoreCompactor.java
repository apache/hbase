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
package org.apache.hadoop.hbase.mob;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.hfile.CorruptHFileException;
import org.apache.hadoop.hbase.regionserver.CellSink;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.regionserver.ShipperListener;
import org.apache.hadoop.hbase.regionserver.StoreFileWriter;
import org.apache.hadoop.hbase.regionserver.compactions.CloseChecker;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionProgress;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputControlUtil;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used for testing only. The main purpose is to emulate
 * random failures during MOB compaction process.
 * Example of usage:
 * <pre>{@code
 * public class SomeTest {
 *
 *   public void initConfiguration(Configuration conf){
 *     conf.set(MobStoreEngine.DEFAULT_MOB_COMPACTOR_CLASS_KEY,
         FaultyMobStoreCompactor.class.getName());
       conf.setDouble("hbase.mob.compaction.fault.probability", 0.1);
 *   }
 * }
 * }</pre>
 * @see org.apache.hadoop.hbase.mob.MobStressToolRunner on how to use and configure
 *   this class.
 *
 */
@InterfaceAudience.Private
public class FaultyMobStoreCompactor extends DefaultMobStoreCompactor {

  private static final Logger LOG = LoggerFactory.getLogger(FaultyMobStoreCompactor.class);

  public static AtomicLong mobCounter = new AtomicLong();
  public static AtomicLong totalFailures = new AtomicLong();
  public static AtomicLong totalCompactions = new AtomicLong();
  public static AtomicLong totalMajorCompactions = new AtomicLong();

  static double failureProb = 0.1d;

  public FaultyMobStoreCompactor(Configuration conf, HStore store) {
    super(conf, store);
    failureProb = conf.getDouble("hbase.mob.compaction.fault.probability", 0.1);
  }

  @Override
  protected boolean performCompaction(FileDetails fd, InternalScanner scanner, CellSink writer,
      long smallestReadPoint, boolean cleanSeqId, ThroughputController throughputController,
      boolean major, int numofFilesToCompact, CompactionProgress progress) throws IOException {

    totalCompactions.incrementAndGet();
    if (major) {
      totalMajorCompactions.incrementAndGet();
    }
    long bytesWrittenProgressForLog = 0;
    long bytesWrittenProgressForShippedCall = 0;
    // Clear old mob references
    mobRefSet.get().clear();
    boolean isUserRequest = userRequest.get();
    boolean compactMOBs = major && isUserRequest;
    boolean discardMobMiss = conf.getBoolean(MobConstants.MOB_UNSAFE_DISCARD_MISS_KEY,
      MobConstants.DEFAULT_MOB_DISCARD_MISS);

    boolean mustFail = false;
    if (compactMOBs) {
      mobCounter.incrementAndGet();
      double dv = ThreadLocalRandom.current().nextDouble();
      if (dv < failureProb) {
        mustFail = true;
        totalFailures.incrementAndGet();
      }
    }

    FileSystem fs = store.getFileSystem();

    // Since scanner.next() can return 'false' but still be delivering data,
    // we have to use a do/while loop.
    List<Cell> cells = new ArrayList<>();
    // Limit to "hbase.hstore.compaction.kv.max" (default 10) to avoid OOME
    long currentTime = EnvironmentEdgeManager.currentTime();
    long lastMillis = 0;
    if (LOG.isDebugEnabled()) {
      lastMillis = currentTime;
    }
    CloseChecker closeChecker = new CloseChecker(conf, currentTime);
    String compactionName = ThroughputControlUtil.getNameForThrottling(store, "compaction");
    long now = 0;
    boolean hasMore;
    Path path = MobUtils.getMobFamilyPath(conf, store.getTableName(), store.getColumnFamilyName());
    byte[] fileName = null;
    StoreFileWriter mobFileWriter = null;
    long mobCells = 0;
    long cellsCountCompactedToMob = 0, cellsCountCompactedFromMob = 0;
    long cellsSizeCompactedToMob = 0, cellsSizeCompactedFromMob = 0;
    boolean finished = false;

    ScannerContext scannerContext =
        ScannerContext.newBuilder().setBatchLimit(compactionKVMax).build();
    throughputController.start(compactionName);
    KeyValueScanner kvs = (scanner instanceof KeyValueScanner) ? (KeyValueScanner) scanner : null;
    long shippedCallSizeLimit =
        (long) numofFilesToCompact * this.store.getColumnFamilyDescriptor().getBlocksize();

    Cell mobCell = null;

    long counter = 0;
    long countFailAt = -1;
    if (mustFail) {
      countFailAt = ThreadLocalRandom.current().nextInt(100); // randomly fail fast
    }

    try {
      try {
        mobFileWriter = mobStore.createWriterInTmp(new Date(fd.latestPutTs), fd.maxKeyCount,
          major ? majorCompactionCompression : minorCompactionCompression,
          store.getRegionInfo().getStartKey(), true);
        fileName = Bytes.toBytes(mobFileWriter.getPath().getName());
      } catch (IOException e) {
        // Bailing out
        LOG.error("Failed to create mob writer, ", e);
        throw e;
      }
      if (compactMOBs) {
        // Add the only reference we get for compact MOB case
        // because new store file will have only one MOB reference
        // in this case - of newly compacted MOB file
        mobRefSet.get().put(store.getTableName(), mobFileWriter.getPath().getName());
      }
      do {
        hasMore = scanner.next(cells, scannerContext);
        currentTime = EnvironmentEdgeManager.currentTime();
        if (LOG.isDebugEnabled()) {
          now = currentTime;
        }
        if (closeChecker.isTimeLimit(store, currentTime)) {
          progress.cancel();
          return false;
        }
        for (Cell c : cells) {
          counter++;
          if (compactMOBs) {
            if (MobUtils.isMobReferenceCell(c)) {
              if (counter == countFailAt) {
                LOG.warn("INJECTED FAULT mobCounter={}", mobCounter.get());
                throw new CorruptHFileException("injected fault");
              }
              String fName = MobUtils.getMobFileName(c);
              // Added to support migration
              try {
                mobCell = mobStore.resolve(c, true, false).getCell();
              } catch (DoNotRetryIOException e) {
                if (discardMobMiss && e.getCause() != null
                  && e.getCause() instanceof FileNotFoundException) {
                  LOG.error("Missing MOB cell: file={} not found cell={}", fName, c);
                  continue;
                } else {
                  throw e;
                }
              }

              if (discardMobMiss && mobCell.getValueLength() == 0) {
                LOG.error("Missing MOB cell value: file={} cell={}", fName, mobCell);
                continue;
              }

              if (mobCell.getValueLength() > mobSizeThreshold) {
                // put the mob data back to the store file
                PrivateCellUtil.setSequenceId(mobCell, c.getSequenceId());
                mobFileWriter.append(mobCell);
                writer.append(
                  MobUtils.createMobRefCell(mobCell, fileName, this.mobStore.getRefCellTags()));
                mobCells++;
              } else {
                // If MOB value is less than threshold, append it directly to a store file
                PrivateCellUtil.setSequenceId(mobCell, c.getSequenceId());
                writer.append(mobCell);
                cellsCountCompactedFromMob++;
                cellsSizeCompactedFromMob += mobCell.getValueLength();
              }
            } else {
              // Not a MOB reference cell
              int size = c.getValueLength();
              if (size > mobSizeThreshold) {
                mobFileWriter.append(c);
                writer
                    .append(MobUtils.createMobRefCell(c, fileName, this.mobStore.getRefCellTags()));
                mobCells++;
                cellsCountCompactedToMob++;
                cellsSizeCompactedToMob += c.getValueLength();
              } else {
                writer.append(c);
              }
            }
          } else if (c.getTypeByte() != KeyValue.Type.Put.getCode()) {
            // Not a major compaction or major with MOB disabled
            // If the kv type is not put, directly write the cell
            // to the store file.
            writer.append(c);
          } else if (MobUtils.isMobReferenceCell(c)) {
            // Not a major MOB compaction, Put MOB reference
            if (MobUtils.hasValidMobRefCellValue(c)) {
              int size = MobUtils.getMobValueLength(c);
              if (size > mobSizeThreshold) {
                // If the value size is larger than the threshold, it's regarded as a mob. Since
                // its value is already in the mob file, directly write this cell to the store file
                Optional<TableName> refTable = MobUtils.getTableName(c);
                if (refTable.isPresent()) {
                  mobRefSet.get().put(refTable.get(), MobUtils.getMobFileName(c));
                  writer.append(c);
                } else {
                  throw new IOException(String.format("MOB cell did not contain a tablename " +
                      "tag. should not be possible. see ref guide on mob troubleshooting. " +
                      "store=%s cell=%s", getStoreInfo(), c));
                }
              } else {
                // If the value is not larger than the threshold, it's not regarded a mob. Retrieve
                // the mob cell from the mob file, and write it back to the store file.
                mobCell = mobStore.resolve(c, true, false).getCell();
                if (mobCell.getValueLength() != 0) {
                  // put the mob data back to the store file
                  PrivateCellUtil.setSequenceId(mobCell, c.getSequenceId());
                  writer.append(mobCell);
                  cellsCountCompactedFromMob++;
                  cellsSizeCompactedFromMob += mobCell.getValueLength();
                } else {
                  // If the value of a file is empty, there might be issues when retrieving,
                  // directly write the cell to the store file, and leave it to be handled by the
                  // next compaction.
                  LOG.error("Empty value for: " + c);
                  Optional<TableName> refTable = MobUtils.getTableName(c);
                  if (refTable.isPresent()) {
                    mobRefSet.get().put(refTable.get(), MobUtils.getMobFileName(c));
                    writer.append(c);
                  } else {
                    throw new IOException(String.format("MOB cell did not contain a tablename " +
                        "tag. should not be possible. see ref guide on mob troubleshooting. " +
                        "store=%s cell=%s", getStoreInfo(), c));
                  }
                }
              }
            } else {
              LOG.error("Corrupted MOB reference: {}", c);
              writer.append(c);
            }
          } else if (c.getValueLength() <= mobSizeThreshold) {
            // If the value size of a cell is not larger than the threshold, directly write it to
            // the store file.
            writer.append(c);
          } else {
            // If the value size of a cell is larger than the threshold, it's regarded as a mob,
            // write this cell to a mob file, and write the path to the store file.
            mobCells++;
            // append the original keyValue in the mob file.
            mobFileWriter.append(c);
            Cell reference = MobUtils.createMobRefCell(c, fileName, this.mobStore.getRefCellTags());
            // write the cell whose value is the path of a mob file to the store file.
            writer.append(reference);
            cellsCountCompactedToMob++;
            cellsSizeCompactedToMob += c.getValueLength();
            // Add ref we get for compact MOB case
            mobRefSet.get().put(store.getTableName(), mobFileWriter.getPath().getName());
          }

          int len = c.getSerializedSize();
          ++progress.currentCompactedKVs;
          progress.totalCompactedSize += len;
          bytesWrittenProgressForShippedCall += len;
          if (LOG.isDebugEnabled()) {
            bytesWrittenProgressForLog += len;
          }
          throughputController.control(compactionName, len);
          if (closeChecker.isSizeLimit(store, len)) {
            progress.cancel();
            return false;
          }
          if (kvs != null && bytesWrittenProgressForShippedCall > shippedCallSizeLimit) {
            ((ShipperListener) writer).beforeShipped();
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
    } catch (FileNotFoundException e) {
      LOG.error("MOB Stress Test FAILED, region: " + store.getRegionInfo().getEncodedName(), e);
      System.exit(-1);
    } catch (IOException t) {
      LOG.error("Mob compaction failed for region: " + store.getRegionInfo().getEncodedName());
      throw t;
    } finally {
      // Clone last cell in the final because writer will append last cell when committing. If
      // don't clone here and once the scanner get closed, then the memory of last cell will be
      // released. (HBASE-22582)
      ((ShipperListener) writer).beforeShipped();
      throughputController.finish(compactionName);
      if (!finished && mobFileWriter != null) {
        // Remove all MOB references because compaction failed
        mobRefSet.get().clear();
        // Abort writer
        abortWriter(mobFileWriter);
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
