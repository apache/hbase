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

import static org.apache.hadoop.hbase.regionserver.ScanType.COMPACT_DROP_DELETES;
import static org.apache.hadoop.hbase.regionserver.ScanType.COMPACT_RETAIN_DELETES;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.TableName;
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
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Compact passed set of files in the mob-enabled column family.
 */
@InterfaceAudience.Private
public class DefaultMobStoreCompactor extends DefaultCompactor {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultMobStoreCompactor.class);
  protected long mobSizeThreshold;
  protected HMobStore mobStore;

  /*
   *  MOB file reference set thread local variable. It contains set of
   *  a MOB file names, which newly compacted store file has references to.
   *  This variable is populated during compaction and the content of it is
   *  written into meta section of a newly created store file at the final step
   *  of compaction process. 
   */
  
  static ThreadLocal<Set<String>> mobRefSet = new ThreadLocal<Set<String>>() {
    @Override
    protected Set<String> initialValue() {
      return new HashSet<String>();
    }
  };

  /*
   * Is it user or system-originated request.
   */
  
  static ThreadLocal<Boolean> userRequest = new ThreadLocal<Boolean>() {
    @Override
    protected Boolean initialValue() {
      return Boolean.FALSE;
    }
  };

  /*
   * Map : MOB file name - file length
   * Can be expensive for large amount of MOB files?
   */
  static ThreadLocal<HashMap<String, Long>> mobLengthMap =
      new ThreadLocal<HashMap<String, Long>>() {
        @Override
        protected HashMap<String, Long> initialValue() {
          return new HashMap<String, Long>();
        }
      };
  
  private final InternalScannerFactory scannerFactory = new InternalScannerFactory() {

    @Override
    public ScanType getScanType(CompactionRequestImpl request) {
      return request.isAllFiles() ? COMPACT_DROP_DELETES : COMPACT_RETAIN_DELETES;
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
  public List<Path> compact(CompactionRequestImpl request,
      ThroughputController throughputController, User user) throws IOException {
    LOG.info("Mob compaction: major=" + request.isMajor() + " isAll=" + request.isAllFiles()
        + " priority=" + request.getPriority());
    if (request.getPriority() == HStore.PRIORITY_USER) {
      userRequest.set(Boolean.TRUE);
    } else {
      userRequest.set(Boolean.FALSE);
    }
    LOG.info("Mob compaction files: " + request.getFiles());
    // Check if I/O optimized MOB compaction
    if (conf.get(MobConstants.MOB_COMPACTION_TYPE_KEY, MobConstants.DEFAULT_MOB_COMPACTION_TYPE)
        .equals(MobConstants.IO_OPTIMIZED_MOB_COMPACTION_TYPE)) {
      if (request.isMajor() && request.getPriority() == HStore.PRIORITY_USER) {
        Path mobDir = MobUtils.getMobFamilyPath(conf, store.getTableName(),
          store.getColumnFamilyName());
        List<Path> mobFiles = MobUtils.getReferencedMobFiles(request.getFiles(), mobDir);
        if (mobFiles.size() > 0) {
          calculateMobLengthMap(mobFiles);
        }
        LOG.info("I/O optimized MOB compaction. Total referenced MOB files: {}", mobFiles.size());
      }
    }
    return compact(request, scannerFactory, writerFactory, throughputController, user);
  }

  private void calculateMobLengthMap(List<Path> mobFiles) throws IOException {
    FileSystem fs = mobFiles.get(0).getFileSystem(this.conf);
    HashMap<String, Long> map = mobLengthMap.get();
    map.clear();
    for (Path p: mobFiles) {
      FileStatus st = fs.getFileStatus(p);
      long size = st.getLen();
      LOG.info("Ref MOB file={} size={}", p, size);
      map.put(p.getName(), fs.getFileStatus(p).getLen());
    }
  }


  /**
   * Performs compaction on a column family with the mob flag enabled.
   * This works only when MOB compaction is explicitly requested (by User), or by Master
   * There are two modes of a MOB compaction:<br>
   * <p>
   * <ul>
   * <li>1. Full mode - when all MOB data for a region is compacted into a single MOB file.
   * <li>2. I/O optimized mode - for use cases with no or infrequent updates/deletes of a <br>
   * MOB data. The main idea behind i/o optimized compaction is to limit maximum size of a MOB 
   * file produced during compaction and to limit I/O write/read amplification.
   * </ul>
   * The basic algorithm of compaction is the following: <br>
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
    // Clear old mob references
    mobRefSet.get().clear();
    boolean isUserRequest = userRequest.get();
    boolean compactMOBs = major && isUserRequest;
    boolean ioOptimizedMode = conf.get(MobConstants.MOB_COMPACTION_TYPE_KEY,
      MobConstants.DEFAULT_MOB_COMPACTION_TYPE)
        .equals(MobConstants.IO_OPTIMIZED_MOB_COMPACTION_TYPE);

    boolean discardMobMiss =
        conf.getBoolean(MobConstants.MOB_UNSAFE_DISCARD_MISS_KEY, 
          MobConstants.DEFAULT_MOB_DISCARD_MISS);

    long maxMobFileSize = conf.getLong(MobConstants.MOB_COMPACTION_MAX_FILE_SIZE_KEY, 
      MobConstants.DEFAULT_MOB_COMPACTION_MAX_FILE_SIZE);
    LOG.info("Compact MOB={} optimized={} maximum MOB file size={} major={}", compactMOBs, 
      ioOptimizedMode, maxMobFileSize, major);
    
    FileSystem fs = FileSystem.get(conf);

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
    StoreFileWriter mobFileWriter = null;
    /*
     * mobCells are used only to decide if we need to commit or abort current MOB output file. 
     */
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
    try {
        
      mobFileWriter = newMobWriter(fd);
      fileName = Bytes.toBytes(mobFileWriter.getPath().getName());

      do {
        hasMore = scanner.next(cells, scannerContext);
        now = EnvironmentEdgeManager.currentTime();
        for (Cell c : cells) {
          if (compactMOBs) {
            if (MobUtils.isMobReferenceCell(c)) {
              String fName = MobUtils.getMobFileName(c);
              Path pp = new Path(new Path(fs.getUri()), new Path(path, fName));

              // Added to support migration
              try {
                mobCell = mobStore.resolve(c, true, false).getCell();
              } catch (FileNotFoundException fnfe) {
                if (discardMobMiss) {
                  LOG.debug("Missing MOB cell: file={} not found cell={}", pp, c);
                  continue;
                } else {
                  throw fnfe;
                }
              }

              if (discardMobMiss && mobCell.getValueLength() == 0) {
                LOG.error("Missing MOB cell value: file=" + pp + " cell=" + mobCell);
                continue;
              } else if (mobCell.getValueLength() == 0) {
                //TODO: what to do here? This is data corruption?
                LOG.warn("Found 0 length MOB cell in a file={} cell={}", pp, mobCell);
              }

              if (mobCell.getValueLength() > mobSizeThreshold) {
                // put the mob data back to the MOB store file
                PrivateCellUtil.setSequenceId(mobCell, c.getSequenceId());
                if (!ioOptimizedMode) {
                  mobFileWriter.append(mobCell);
                  mobCells++;
                  writer.append(MobUtils.createMobRefCell(mobCell, fileName,
                    this.mobStore.getRefCellTags()));
                } else {
                  // I/O optimized mode
                  // Check if MOB cell origin file size is 
                  // greater than threshold
                  Long size = mobLengthMap.get().get(fName);
                  if (size == null) {
                    // FATAL error, abort compaction
                    String msg = 
                        String.format("Found unreferenced MOB file during compaction %s, aborting.",
                      fName);
                    LOG.error(msg);
                    throw new IOException(msg);
                  }
                  // Can not be null
                  if (size < maxMobFileSize) {
                    // If MOB cell origin file is below threshold
                    // it is get compacted 
                    mobFileWriter.append(mobCell);
                    // Update number of mobCells in a current mob writer
                    mobCells++;
                    writer.append(MobUtils.createMobRefCell(mobCell, fileName,
                      this.mobStore.getRefCellTags()));                   
                    // Update total size of the output (we do not take into account 
                    // file compression yet)
                    long len = getLength(mobFileWriter);
                    
                    if (len > maxMobFileSize) {
                      LOG.debug("Closing output MOB File, length={} file={}", 
                        len, Bytes.toString(fileName));
                      commitOrAbortMobWriter(mobFileWriter, fd.maxSeqId, mobCells, major);
                      mobFileWriter = newMobWriter(fd);
                      fileName = Bytes.toBytes(mobFileWriter.getPath().getName());
                      mobCells = 0;
                    }
                  } else {
                    // We leave large MOB file as is (is not compacted),
                    // then we update set of MOB file references
                    // and append mob cell directly to the store's writer
                    mobRefSet.get().add(fName);
                    writer.append(mobCell);
                  }
                }
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
                // This MOB cell comes from a regular store file
                // therefore we store it into original mob output
                mobFileWriter.append(c);
                writer
                    .append(MobUtils.createMobRefCell(c, fileName, this.mobStore.getRefCellTags()));
                mobCells++;
                cellsCountCompactedToMob++;
                cellsSizeCompactedToMob += c.getValueLength();
                if (ioOptimizedMode) {
                  // Update total size of the output (we do not take into account 
                  // file compression yet)
                  long len = getLength(mobFileWriter);
                  if (len > maxMobFileSize) {
                    commitOrAbortMobWriter(mobFileWriter, fd.maxSeqId, mobCells, major);
                    mobFileWriter = newMobWriter(fd);
                    fileName = Bytes.toBytes(mobFileWriter.getPath().getName());
                    mobCells = 0;
                  }         
                }
              } else {
                // Not a MOB cell, write it directly to a store file
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
              // We do not check mobSizeThreshold during normal compaction,
              // leaving it to a MOB compaction run
              writer.append(c);
              // Add MOB reference to a MOB reference set
              mobRefSet.get().add(MobUtils.getMobFileName(c));              
            } else {
              // TODO ????
              LOG.error("Corrupted MOB reference: " + c);
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
            if (ioOptimizedMode) {
              long len = getLength(mobFileWriter);
              if (len > maxMobFileSize) {
                commitOrAbortMobWriter(mobFileWriter, fd.maxSeqId, mobCells, major);
                mobFileWriter = newMobWriter(fd);
                fileName = Bytes.toBytes(mobFileWriter.getPath().getName());
                mobCells = 0;
              }         
            }
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
    } catch (IOException t) {
      LOG.error("Mob compaction failed for region:{} ", store.getRegionInfo().getEncodedName());
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
        LOG.debug("Aborting writer for {} because of a compaction failure", 
          mobFileWriter.getPath());
        abortWriter(mobFileWriter);
      }
    }
    
    // Commit last MOB writer
    commitOrAbortMobWriter(mobFileWriter, fd.maxSeqId, mobCells, major);

    mobStore.updateCellsCountCompactedFromMob(cellsCountCompactedFromMob);
    mobStore.updateCellsCountCompactedToMob(cellsCountCompactedToMob);
    mobStore.updateCellsSizeCompactedFromMob(cellsSizeCompactedFromMob);
    mobStore.updateCellsSizeCompactedToMob(cellsSizeCompactedToMob);
    progress.complete();
    return true;
  }

  private long getLength(StoreFileWriter mobFileWriter) throws IOException {
    return mobFileWriter.getPos();
  }


  private StoreFileWriter newMobWriter(FileDetails fd/*, boolean compactMOBs*/) 
      throws IOException {
    try {
      StoreFileWriter mobFileWriter = mobStore.createWriterInTmp(new Date(fd.latestPutTs), 
        fd.maxKeyCount, compactionCompression, store.getRegionInfo().getStartKey(), true);
      LOG.debug("New MOB writer created={}", mobFileWriter.getPath().getName());
      // Add reference we get for compact MOB
      mobRefSet.get().add(mobFileWriter.getPath().getName());
      return mobFileWriter;
    } catch (IOException e) {
      // Bailing out
      LOG.error("Failed to create mob writer, ", e);
      throw e;
    }
  }
  
  private void commitOrAbortMobWriter(StoreFileWriter mobFileWriter, long maxSeqId, 
     long mobCells, boolean major) throws IOException
  {
    // Commit or abort major mob writer
    // If IOException happens during below operation, some 
    // MOB files can be committed partially, but corresponding 
    // store file won't be committed, therefore these MOB files
    // become orphans and will be deleted during next MOB cleaning chore cycle
    LOG.debug("Commit or abort size={} mobCells={} major={} file={}",
      mobFileWriter.getPos(), mobCells, major, mobFileWriter.getPath().getName());
    Path path = MobUtils.getMobFamilyPath(conf, store.getTableName(), store.getColumnFamilyName());
    if (mobFileWriter != null) {
      if (mobCells > 0) {
        // If the mob file is not empty, commit it.
        mobFileWriter.appendMetadata(maxSeqId, major, mobCells);
        mobFileWriter.close();
        mobStore.commitFile(mobFileWriter.getPath(), path);
      } else {
        // If the mob file is empty, delete it instead of committing.
        LOG.debug("Aborting writer for {} because there are no MOB cells", 
          mobFileWriter.getPath());
        // Remove MOB file from reference set
        mobRefSet.get().remove(mobFileWriter.getPath().getName());
        abortWriter(mobFileWriter);
      }
    }
  }
  
  protected static String createKey(TableName tableName, String encodedName,
      String columnFamilyName) {
    return tableName.getNameAsString()+ "_" + encodedName + "_"+ columnFamilyName;
  }

  @Override
  protected List<Path> commitWriter(StoreFileWriter writer, FileDetails fd,
      CompactionRequestImpl request) throws IOException {
    List<Path> newFiles = Lists.newArrayList(writer.getPath());
    writer.appendMetadata(fd.maxSeqId, request.isAllFiles(), request.getFiles());
    // Append MOB references
    Set<String> refSet = mobRefSet.get();
    writer.appendMobMetadata(refSet);
    writer.close();
    return newFiles;
  }

}

