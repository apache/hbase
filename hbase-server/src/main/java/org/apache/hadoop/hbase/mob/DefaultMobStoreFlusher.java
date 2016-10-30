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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ArrayBackedTag;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.TagType;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.regionserver.DefaultStoreFlusher;
import org.apache.hadoop.hbase.regionserver.HMobStore;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.MemStoreSnapshot;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFileWriter;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;

/**
 * An implementation of the StoreFlusher. It extends the DefaultStoreFlusher.
 * If the store is not a mob store, the flusher flushes the MemStore the same with
 * DefaultStoreFlusher,
 * If the store is a mob store, the flusher flushes the MemStore into two places.
 * One is the store files of HBase, the other is the mob files.
 * <ol>
 * <li>Cells that are not PUT type or have the delete mark will be directly flushed to HBase.</li>
 * <li>If the size of a cell value is larger than a threshold, it'll be flushed
 * to a mob file, another cell with the path of this file will be flushed to HBase.</li>
 * <li>If the size of a cell value is smaller than or equal with a threshold, it'll be flushed to
 * HBase directly.</li>
 * </ol>
 *
 */
@InterfaceAudience.Private
public class DefaultMobStoreFlusher extends DefaultStoreFlusher {

  private static final Log LOG = LogFactory.getLog(DefaultMobStoreFlusher.class);
  private final Object flushLock = new Object();
  private long mobCellValueSizeThreshold = 0;
  private Path targetPath;
  private HMobStore mobStore;

  public DefaultMobStoreFlusher(Configuration conf, Store store) throws IOException {
    super(conf, store);
    mobCellValueSizeThreshold = store.getFamily().getMobThreshold();
    this.targetPath = MobUtils.getMobFamilyPath(conf, store.getTableName(),
        store.getColumnFamilyName());
    if (!this.store.getFileSystem().exists(targetPath)) {
      this.store.getFileSystem().mkdirs(targetPath);
    }
    this.mobStore = (HMobStore) store;
  }

  /**
   * Flushes the snapshot of the MemStore.
   * If this store is not a mob store, flush the cells in the snapshot to store files of HBase.
   * If the store is a mob one, the flusher flushes the MemStore into two places.
   * One is the store files of HBase, the other is the mob files.
   * <ol>
   * <li>Cells that are not PUT type or have the delete mark will be directly flushed to
   * HBase.</li>
   * <li>If the size of a cell value is larger than a threshold, it'll be
   * flushed to a mob file, another cell with the path of this file will be flushed to HBase.</li>
   * <li>If the size of a cell value is smaller than or equal with a threshold, it'll be flushed to
   * HBase directly.</li>
   * </ol>
   */
  @Override
  public List<Path> flushSnapshot(MemStoreSnapshot snapshot, long cacheFlushId,
      MonitoredTask status, ThroughputController throughputController) throws IOException {
    ArrayList<Path> result = new ArrayList<Path>();
    long cellsCount = snapshot.getCellsCount();
    if (cellsCount == 0) return result; // don't flush if there are no entries

    // Use a store scanner to find which rows to flush.
    long smallestReadPoint = store.getSmallestReadPoint();
    InternalScanner scanner = createScanner(snapshot.getScanner(), smallestReadPoint);
    if (scanner == null) {
      return result; // NULL scanner returned from coprocessor hooks means skip normal processing
    }
    StoreFileWriter writer;
    try {
      // TODO: We can fail in the below block before we complete adding this flush to
      // list of store files. Add cleanup of anything put on filesystem if we fail.
      synchronized (flushLock) {
        status.setStatus("Flushing " + store + ": creating writer");
        // Write the map out to the disk
        writer = store.createWriterInTmp(cellsCount, store.getFamily().getCompression(),
            false, true, true, false/*default for dropbehind*/, snapshot.getTimeRangeTracker());
        try {
          // It's a mob store, flush the cells in a mob way. This is the difference of flushing
          // between a normal and a mob store.
          performMobFlush(snapshot, cacheFlushId, scanner, writer, status);
        } finally {
          finalizeWriter(writer, cacheFlushId, status);
        }
      }
    } finally {
      scanner.close();
    }
    LOG.info("Mob store is flushed, sequenceid=" + cacheFlushId + ", memsize="
        + StringUtils.TraditionalBinaryPrefix.long2String(snapshot.getDataSize(), "", 1) +
        ", hasBloomFilter=" + writer.hasGeneralBloom() +
        ", into tmp file " + writer.getPath());
    result.add(writer.getPath());
    return result;
  }

  /**
   * Flushes the cells in the mob store.
   * <ol>In the mob store, the cells with PUT type might have or have no mob tags.
   * <li>If a cell does not have a mob tag, flushing the cell to different files depends
   * on the value length. If the length is larger than a threshold, it's flushed to a
   * mob file and the mob file is flushed to a store file in HBase. Otherwise, directly
   * flush the cell to a store file in HBase.</li>
   * <li>If a cell have a mob tag, its value is a mob file name, directly flush it
   * to a store file in HBase.</li>
   * </ol>
   * @param snapshot Memstore snapshot.
   * @param cacheFlushId Log cache flush sequence number.
   * @param scanner The scanner of memstore snapshot.
   * @param writer The store file writer.
   * @param status Task that represents the flush operation and may be updated with status.
   * @throws IOException
   */
  protected void performMobFlush(MemStoreSnapshot snapshot, long cacheFlushId,
      InternalScanner scanner, StoreFileWriter writer, MonitoredTask status) throws IOException {
    StoreFileWriter mobFileWriter = null;
    int compactionKVMax = conf.getInt(HConstants.COMPACTION_KV_MAX,
        HConstants.COMPACTION_KV_MAX_DEFAULT);
    long mobCount = 0;
    long mobSize = 0;
    long time = snapshot.getTimeRangeTracker().getMax();
    mobFileWriter = mobStore.createWriterInTmp(new Date(time), snapshot.getCellsCount(),
        store.getFamily().getCompression(), store.getRegionInfo().getStartKey());
    // the target path is {tableName}/.mob/{cfName}/mobFiles
    // the relative path is mobFiles
    byte[] fileName = Bytes.toBytes(mobFileWriter.getPath().getName());
    try {
      Tag tableNameTag = new ArrayBackedTag(TagType.MOB_TABLE_NAME_TAG_TYPE,
          store.getTableName().getName());
      List<Cell> cells = new ArrayList<Cell>();
      boolean hasMore;
      ScannerContext scannerContext =
              ScannerContext.newBuilder().setBatchLimit(compactionKVMax).build();

      do {
        hasMore = scanner.next(cells, scannerContext);
        if (!cells.isEmpty()) {
          for (Cell c : cells) {
            // If we know that this KV is going to be included always, then let us
            // set its memstoreTS to 0. This will help us save space when writing to
            // disk.
            if (c.getValueLength() <= mobCellValueSizeThreshold || MobUtils.isMobReferenceCell(c)
                || c.getTypeByte() != KeyValue.Type.Put.getCode()) {
              writer.append(c);
            } else {
              // append the original keyValue in the mob file.
              mobFileWriter.append(c);
              mobSize += c.getValueLength();
              mobCount++;

              // append the tags to the KeyValue.
              // The key is same, the value is the filename of the mob file
              KeyValue reference = MobUtils.createMobRefKeyValue(c, fileName, tableNameTag);
              writer.append(reference);
            }
          }
          cells.clear();
        }
      } while (hasMore);
    } finally {
      status.setStatus("Flushing mob file " + store + ": appending metadata");
      mobFileWriter.appendMetadata(cacheFlushId, false, mobCount);
      status.setStatus("Flushing mob file " + store + ": closing flushed file");
      mobFileWriter.close();
    }

    if (mobCount > 0) {
      // commit the mob file from temp folder to target folder.
      // If the mob file is committed successfully but the store file is not,
      // the committed mob file will be handled by the sweep tool as an unused
      // file.
      mobStore.commitFile(mobFileWriter.getPath(), targetPath);
      mobStore.updateMobFlushCount();
      mobStore.updateMobFlushedCellsCount(mobCount);
      mobStore.updateMobFlushedCellsSize(mobSize);
    } else {
      try {
        // If the mob file is empty, delete it instead of committing.
        store.getFileSystem().delete(mobFileWriter.getPath(), true);
      } catch (IOException e) {
        LOG.error("Failed to delete the temp mob file", e);
      }
    }
  }
}
