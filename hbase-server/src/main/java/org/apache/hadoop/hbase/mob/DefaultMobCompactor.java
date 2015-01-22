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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.TagType;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HMobStore;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.MobCompactionStoreScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFile.Writer;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;
import org.apache.hadoop.hbase.regionserver.compactions.DefaultCompactor;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Compact passed set of files in the mob-enabled column family.
 */
@InterfaceAudience.Private
public class DefaultMobCompactor extends DefaultCompactor {

  private static final Log LOG = LogFactory.getLog(DefaultMobCompactor.class);
  private long mobSizeThreshold;
  private HMobStore mobStore;
  public DefaultMobCompactor(Configuration conf, Store store) {
    super(conf, store);
    // The mob cells reside in the mob-enabled column family which is held by HMobStore.
    // During the compaction, the compactor reads the cells from the mob files and
    // probably creates new mob files. All of these operations are included in HMobStore,
    // so we need to cast the Store to HMobStore.
    if (!(store instanceof HMobStore)) {
      throw new IllegalArgumentException("The store " + store + " is not a HMobStore");
    }
    mobStore = (HMobStore) store;
    mobSizeThreshold = store.getFamily().getMobThreshold();
  }

  /**
   * Creates a writer for a new file in a temporary directory.
   * @param fd The file details.
   * @param smallestReadPoint The smallest mvcc readPoint across all the scanners in this region.
   * @return Writer for a new StoreFile in the tmp dir.
   * @throws IOException
   */
  @Override
  protected Writer createTmpWriter(FileDetails fd, long smallestReadPoint) throws IOException {
    // make this writer with tags always because of possible new cells with tags.
    StoreFile.Writer writer = store.createWriterInTmp(fd.maxKeyCount, this.compactionCompression,
        true, fd.maxMVCCReadpoint >= smallestReadPoint, true);
    return writer;
  }

  @Override
  protected InternalScanner createScanner(Store store, List<StoreFileScanner> scanners,
      ScanType scanType, long smallestReadPoint, long earliestPutTs) throws IOException {
    Scan scan = new Scan();
    scan.setMaxVersions(store.getFamily().getMaxVersions());
    if (scanType == ScanType.COMPACT_DROP_DELETES) {
      scanType = ScanType.COMPACT_RETAIN_DELETES;
      return new MobCompactionStoreScanner(store, store.getScanInfo(), scan, scanners,
          scanType, smallestReadPoint, earliestPutTs, true);
    } else {
      return new MobCompactionStoreScanner(store, store.getScanInfo(), scan, scanners,
          scanType, smallestReadPoint, earliestPutTs, false);
    }
  }

  /**
   * Performs compaction on a column family with the mob flag enabled.
   * This is for when the mob threshold size has changed or if the mob
   * column family mode has been toggled via an alter table statement.
   * Compacts the files by the following rules.
   * 1. If the cell has a mob reference tag, the cell's value is the path of the mob file.
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
   * 2. If the cell doesn't have a reference tag.
   * <ol>
   * <li>
   * If the value size of a cell is larger than the threshold, this cell is regarded as a mob,
   * write this cell to a mob file, and write the path of this mob file to the store file.
   * </li>
   * <li>
   * Otherwise, directly write this cell into the store file.
   * </li>
   * </ol>
   * In the mob compaction, the {@link MobCompactionStoreScanner} is used as a scanner
   * which could output the normal cells and delete markers together when required.
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
   * @param major Is a major compaction.
   * @return Whether compaction ended; false if it was interrupted for any reason.
   */
  @Override
  protected boolean performCompaction(FileDetails fd, InternalScanner scanner, CellSink writer,
      long smallestReadPoint, boolean cleanSeqId, boolean major) throws IOException {
    if (!(scanner instanceof MobCompactionStoreScanner)) {
      throw new IllegalArgumentException(
          "The scanner should be an instance of MobCompactionStoreScanner");
    }
    MobCompactionStoreScanner compactionScanner = (MobCompactionStoreScanner) scanner;
    int bytesWritten = 0;
    // Since scanner.next() can return 'false' but still be delivering data,
    // we have to use a do/while loop.
    List<Cell> cells = new ArrayList<Cell>();
    // Limit to "hbase.hstore.compaction.kv.max" (default 10) to avoid OOME
    int closeCheckInterval = HStore.getCloseCheckInterval();
    boolean hasMore;
    Path path = MobUtils.getMobFamilyPath(conf, store.getTableName(), store.getColumnFamilyName());
    byte[] fileName = null;
    StoreFile.Writer mobFileWriter = null;
    StoreFile.Writer delFileWriter = null;
    long mobCells = 0;
    long deleteMarkersCount = 0;
    Tag tableNameTag = new Tag(TagType.MOB_TABLE_NAME_TAG_TYPE, store.getTableName()
            .getName());
    long mobCompactedIntoMobCellsCount = 0;
    long mobCompactedFromMobCellsCount = 0;
    long mobCompactedIntoMobCellsSize = 0;
    long mobCompactedFromMobCellsSize = 0;
    try {
      try {
        // If the mob file writer could not be created, directly write the cell to the store file.
        mobFileWriter = mobStore.createWriterInTmp(new Date(fd.latestPutTs), fd.maxKeyCount,
            store.getFamily().getCompression(), store.getRegionInfo().getStartKey());
        fileName = Bytes.toBytes(mobFileWriter.getPath().getName());
      } catch (IOException e) {
        LOG.error(
            "Fail to create mob writer, "
                + "we will continue the compaction by writing MOB cells directly in store files",
            e);
      }
      delFileWriter = mobStore.createDelFileWriterInTmp(new Date(fd.latestPutTs), fd.maxKeyCount,
          store.getFamily().getCompression(), store.getRegionInfo().getStartKey());
      do {
        hasMore = compactionScanner.next(cells, compactionKVMax);
        // output to writer:
        for (Cell c : cells) {
          // TODO remove the KeyValueUtil.ensureKeyValue before merging back to trunk.
          KeyValue kv = KeyValueUtil.ensureKeyValue(c);
          resetSeqId(smallestReadPoint, cleanSeqId, kv);
          if (compactionScanner.isOutputDeleteMarkers() && CellUtil.isDelete(c)) {
            delFileWriter.append(kv);
            deleteMarkersCount++;
          } else if (mobFileWriter == null || kv.getTypeByte() != KeyValue.Type.Put.getCode()) {
            // If the mob file writer is null or the kv type is not put, directly write the cell
            // to the store file.
            writer.append(kv);
          } else if (MobUtils.isMobReferenceCell(kv)) {
            if (MobUtils.hasValidMobRefCellValue(kv)) {
              int size = MobUtils.getMobValueLength(kv);
              if (size > mobSizeThreshold) {
                // If the value size is larger than the threshold, it's regarded as a mob. Since
                // its value is already in the mob file, directly write this cell to the store file
                writer.append(kv);
              } else {
                // If the value is not larger than the threshold, it's not regarded a mob. Retrieve
                // the mob cell from the mob file, and write it back to the store file.
                Cell cell = mobStore.resolve(kv, false);
                if (cell.getValueLength() != 0) {
                  // put the mob data back to the store file
                  KeyValue mobKv = KeyValueUtil.ensureKeyValue(cell);
                  mobKv.setSequenceId(kv.getSequenceId());
                  writer.append(mobKv);
                  mobCompactedFromMobCellsCount++;
                  mobCompactedFromMobCellsSize += cell.getValueLength();
                } else {
                  // If the value of a file is empty, there might be issues when retrieving,
                  // directly write the cell to the store file, and leave it to be handled by the
                  // next compaction.
                  writer.append(kv);
                }
              }
            } else {
              LOG.warn("The value format of the KeyValue " + kv
                  + " is wrong, its length is less than " + Bytes.SIZEOF_INT);
              writer.append(kv);
            }
          } else if (kv.getValueLength() <= mobSizeThreshold) {
            // If the value size of a cell is not larger than the threshold, directly write it to
            // the store file.
            writer.append(kv);
          } else {
            // If the value size of a cell is larger than the threshold, it's regarded as a mob,
            // write this cell to a mob file, and write the path to the store file.
            mobCells++;
            // append the original keyValue in the mob file.
            mobFileWriter.append(kv);
            KeyValue reference = MobUtils.createMobRefKeyValue(kv, fileName, tableNameTag);
            // write the cell whose value is the path of a mob file to the store file.
            writer.append(reference);
            mobCompactedIntoMobCellsCount++;
            mobCompactedIntoMobCellsSize += kv.getValueLength();
          }
          ++progress.currentCompactedKVs;

          // check periodically to see if a system stop is requested
          if (closeCheckInterval > 0) {
            bytesWritten += kv.getLength();
            if (bytesWritten > closeCheckInterval) {
              bytesWritten = 0;
              if (!store.areWritesEnabled()) {
                progress.cancel();
                return false;
              }
            }
          }
        }
        cells.clear();
      } while (hasMore);
    } finally {
      if (mobFileWriter != null) {
        mobFileWriter.appendMetadata(fd.maxSeqId, major, mobCells);
        mobFileWriter.close();
      }
      if (delFileWriter != null) {
        delFileWriter.appendMetadata(fd.maxSeqId, major, deleteMarkersCount);
        delFileWriter.close();
      }
    }
    if (mobFileWriter != null) {
      if (mobCells > 0) {
        // If the mob file is not empty, commit it.
        mobStore.commitFile(mobFileWriter.getPath(), path);
      } else {
        try {
          // If the mob file is empty, delete it instead of committing.
          store.getFileSystem().delete(mobFileWriter.getPath(), true);
        } catch (IOException e) {
          LOG.error("Fail to delete the temp mob file", e);
        }
      }
    }
    if (delFileWriter != null) {
      if (deleteMarkersCount > 0) {
        // If the del file is not empty, commit it.
        // If the commit fails, the compaction is re-performed again.
        mobStore.commitFile(delFileWriter.getPath(), path);
      } else {
        try {
          // If the del file is empty, delete it instead of committing.
          store.getFileSystem().delete(delFileWriter.getPath(), true);
        } catch (IOException e) {
          LOG.error("Fail to delete the temp del file", e);
        }
      }
    }
    mobStore.updateMobCompactedFromMobCellsCount(mobCompactedFromMobCellsCount);
    mobStore.updateMobCompactedIntoMobCellsCount(mobCompactedIntoMobCellsCount);
    mobStore.updateMobCompactedFromMobCellsSize(mobCompactedFromMobCellsSize);
    mobStore.updateMobCompactedIntoMobCellsSize(mobCompactedIntoMobCellsSize);
    progress.complete();
    return true;
  }
}
