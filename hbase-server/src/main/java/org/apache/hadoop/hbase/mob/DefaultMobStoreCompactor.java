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
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Consumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.CellSink;
import org.apache.hadoop.hbase.regionserver.HMobStore;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.ScanInfo;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.regionserver.ShipperListener;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;
import org.apache.hadoop.hbase.regionserver.StoreFileWriter;
import org.apache.hadoop.hbase.regionserver.StoreScanner;
import org.apache.hadoop.hbase.regionserver.compactions.CloseChecker;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionProgress;
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

import org.apache.hbase.thirdparty.com.google.common.collect.HashMultimap;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableSetMultimap;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.collect.SetMultimap;

/**
 * Compact passed set of files in the mob-enabled column family.
 */
@InterfaceAudience.Private
public class DefaultMobStoreCompactor extends DefaultCompactor {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultMobStoreCompactor.class);
  protected long mobSizeThreshold;
  protected HMobStore mobStore;
  protected boolean ioOptimizedMode = false;

  /*
   * MOB file reference set thread local variable. It contains set of a MOB file names, which newly
   * compacted store file has references to. This variable is populated during compaction and the
   * content of it is written into meta section of a newly created store file at the final step of
   * compaction process.
   */
  static ThreadLocal<SetMultimap<TableName,String>> mobRefSet =
      ThreadLocal.withInitial(HashMultimap::create);

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
   * Disable IO mode. IO mode can be forcefully disabled if compactor finds
   * old MOB file (pre-distributed compaction). This means that migration has not
   * been completed yet. During data migration (upgrade) process only general compaction
   * is allowed.
   *
   */

  static ThreadLocal<Boolean> disableIO = new ThreadLocal<Boolean>() {

    @Override
    protected Boolean initialValue() {
      return Boolean.FALSE;
    }
  };

  /*
   * Map : MOB file name - file length Can be expensive for large amount of MOB files.
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
        boolean shouldDropBehind, boolean major, Consumer<Path> writerCreationTracker)
        throws IOException {
        // make this writer with tags always because of possible new cells with tags.
        return store.getStoreEngine()
          .createWriter(
            createParams(fd, shouldDropBehind, major, writerCreationTracker)
              .includeMVCCReadpoint(true)
              .includesTag(true));
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
    this.mobStore = (HMobStore) store;
    this.mobSizeThreshold = store.getColumnFamilyDescriptor().getMobThreshold();
    this.ioOptimizedMode = conf.get(MobConstants.MOB_COMPACTION_TYPE_KEY,
      MobConstants.DEFAULT_MOB_COMPACTION_TYPE).
        equals(MobConstants.OPTIMIZED_MOB_COMPACTION_TYPE);

  }

  @Override
  public List<Path> compact(CompactionRequestImpl request,
      ThroughputController throughputController, User user) throws IOException {
    String tableName = store.getTableName().toString();
    String regionName = store.getRegionInfo().getRegionNameAsString();
    String familyName = store.getColumnFamilyName();
    LOG.info("MOB compaction: major={} isAll={} priority={} throughput controller={}" +
      " table={} cf={} region={}",
      request.isMajor(), request.isAllFiles(), request.getPriority(),
      throughputController, tableName, familyName, regionName);
    if (request.getPriority() == HStore.PRIORITY_USER) {
      userRequest.set(Boolean.TRUE);
    } else {
      userRequest.set(Boolean.FALSE);
    }
    LOG.debug("MOB compaction table={} cf={} region={} files: {}", tableName, familyName,
      regionName, request.getFiles());
    // Check if I/O optimized MOB compaction
    if (ioOptimizedMode) {
      if (request.isMajor() && request.getPriority() == HStore.PRIORITY_USER) {
        try {
          final SetMultimap<TableName, String> mobRefs = request.getFiles().stream()
              .map(file -> {
                byte[] value = file.getMetadataValue(HStoreFile.MOB_FILE_REFS);
                ImmutableSetMultimap.Builder<TableName, String> builder;
                if (value == null) {
                  builder = ImmutableSetMultimap.builder();
                } else {
                  try {
                    builder = MobUtils.deserializeMobFileRefs(value);
                  } catch (RuntimeException exception) {
                    throw new RuntimeException("failure getting mob references for hfile " + file,
                        exception);
                  }
                }
                return builder;
              }).reduce((a, b) -> a.putAll(b.build())).orElseGet(ImmutableSetMultimap::builder)
              .build();
          //reset disableIO
          disableIO.set(Boolean.FALSE);
          if (!mobRefs.isEmpty()) {
            calculateMobLengthMap(mobRefs);
          }
          LOG.info("Table={} cf={} region={}. I/O optimized MOB compaction. "+
              "Total referenced MOB files: {}", tableName, familyName, regionName, mobRefs.size());
        } catch (RuntimeException exception) {
          throw new IOException("Failed to get list of referenced hfiles for request " + request,
              exception);
        }
      }
    }

    return compact(request, scannerFactory, writerFactory, throughputController, user);
  }

  /**
   * @param mobRefs multimap of original table name -> mob hfile
   */
  private void calculateMobLengthMap(SetMultimap<TableName, String> mobRefs) throws IOException {
    FileSystem fs = store.getFileSystem();
    HashMap<String, Long> map = mobLengthMap.get();
    map.clear();
    for (Entry<TableName, String> reference : mobRefs.entries()) {
      final TableName table = reference.getKey();
      final String mobfile = reference.getValue();
      if (MobFileName.isOldMobFileName(mobfile)) {
        disableIO.set(Boolean.TRUE);
      }
      List<Path> locations = mobStore.getLocations(table);
      for (Path p : locations) {
        try {
          FileStatus st = fs.getFileStatus(new Path(p, mobfile));
          long size = st.getLen();
          LOG.debug("Referenced MOB file={} size={}", mobfile, size);
          map.put(mobfile, size);
          break;
        } catch (FileNotFoundException exception) {
          LOG.debug("Mob file {} was not in location {}. May have other locations to try.", mobfile,
              p);
        }
      }
      if (!map.containsKey(mobfile)) {
        throw new FileNotFoundException("Could not find mob file " + mobfile + " in the list of " +
            "expected locations: " + locations);
      }
    }
  }

  /**
   * Performs compaction on a column family with the mob flag enabled. This works only when MOB
   * compaction is explicitly requested (by User), or by Master There are two modes of a MOB
   * compaction:<br>
   * <p>
   * <ul>
   * <li>1. Full mode - when all MOB data for a region is compacted into a single MOB file.
   * <li>2. I/O optimized mode - for use cases with no or infrequent updates/deletes of a <br>
   * MOB data. The main idea behind i/o optimized compaction is to limit maximum size of a MOB file
   * produced during compaction and to limit I/O write/read amplification.
   * </ul>
   * The basic algorithm of compaction is the following: <br>
   * 1. If the Put cell has a mob reference tag, the cell's value is the path of the mob file.
   * <ol>
   * <li>If the value size of a cell is larger than the threshold, this cell is regarded as a mob,
   * directly copy the (with mob tag) cell into the new store file.</li>
   * <li>Otherwise, retrieve the mob cell from the mob file, and writes a copy of the cell into the
   * new store file.</li>
   * </ol>
   * 2. If the Put cell doesn't have a reference tag.
   * <ol>
   * <li>If the value size of a cell is larger than the threshold, this cell is regarded as a mob,
   * write this cell to a mob file, and write the path of this mob file to the store file.</li>
   * <li>Otherwise, directly write this cell into the store file.</li>
   * </ol>
   * @param fd File details
   * @param scanner Where to read from.
   * @param writer Where to write to.
   * @param smallestReadPoint Smallest read point.
   * @param cleanSeqId When true, remove seqId(used to be mvcc) value which is <= smallestReadPoint
   * @param throughputController The compaction throughput controller.
   * @param major Is a major compaction.
   * @param numofFilesToCompact the number of files to compact
   * @param progress Progress reporter.
   * @return Whether compaction ended; false if it was interrupted for any reason.
   */
  @Override
  protected boolean performCompaction(FileDetails fd, InternalScanner scanner, CellSink writer,
      long smallestReadPoint, boolean cleanSeqId, ThroughputController throughputController,
      boolean major, int numofFilesToCompact, CompactionProgress progress) throws IOException {
    long bytesWrittenProgressForLog = 0;
    long bytesWrittenProgressForShippedCall = 0;
    // Clear old mob references
    mobRefSet.get().clear();
    boolean isUserRequest = userRequest.get();
    boolean compactMOBs = major && isUserRequest;
    boolean discardMobMiss = conf.getBoolean(MobConstants.MOB_UNSAFE_DISCARD_MISS_KEY,
      MobConstants.DEFAULT_MOB_DISCARD_MISS);
    if (discardMobMiss) {
      LOG.warn("{}=true. This is unsafe setting recommended only when first upgrading to a version"+
        " with the distributed mob compaction feature on a cluster that has experienced MOB data " +
        "corruption.", MobConstants.MOB_UNSAFE_DISCARD_MISS_KEY);
    }
    long maxMobFileSize = conf.getLong(MobConstants.MOB_COMPACTION_MAX_FILE_SIZE_KEY,
      MobConstants.DEFAULT_MOB_COMPACTION_MAX_FILE_SIZE);
    boolean ioOptimizedMode = this.ioOptimizedMode && !disableIO.get();
    LOG.info("Compact MOB={} optimized configured={} optimized enabled={} maximum MOB file size={}"
      + " major={} store={}", compactMOBs,
      this.ioOptimizedMode, ioOptimizedMode, maxMobFileSize, major, getStoreInfo());
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

      mobFileWriter = newMobWriter(fd, major);
      fileName = Bytes.toBytes(mobFileWriter.getPath().getName());

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
          if (compactMOBs) {
            if (MobUtils.isMobReferenceCell(c)) {
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
                LOG.error("Missing MOB cell value: file={} mob cell={} cell={}", fName,
                  mobCell, c);
                continue;
              } else if (mobCell.getValueLength() == 0) {
                String errMsg = String.format("Found 0 length MOB cell in a file=%s mob cell=%s "
                    + " cell=%s",
                  fName, mobCell, c);
                throw new IOException(errMsg);
              }

              if (mobCell.getValueLength() > mobSizeThreshold) {
                // put the mob data back to the MOB store file
                PrivateCellUtil.setSequenceId(mobCell, c.getSequenceId());
                if (!ioOptimizedMode) {
                  mobFileWriter.append(mobCell);
                  mobCells++;
                  writer.append(
                    MobUtils.createMobRefCell(mobCell, fileName, this.mobStore.getRefCellTags()));
                } else {
                  // I/O optimized mode
                  // Check if MOB cell origin file size is
                  // greater than threshold
                  Long size = mobLengthMap.get().get(fName);
                  if (size == null) {
                    // FATAL error (we should never get here though), abort compaction
                    // This error means that meta section of store file does not contain
                    // MOB file, which has references in at least one cell from this store file
                    String msg = String.format(
                      "Found an unexpected MOB file during compaction %s, aborting compaction %s",
                      fName, getStoreInfo());
                    throw new IOException(msg);
                  }
                  // Can not be null
                  if (size < maxMobFileSize) {
                    // If MOB cell origin file is below threshold
                    // it is get compacted
                    mobFileWriter.append(mobCell);
                    // Update number of mobCells in a current mob writer
                    mobCells++;
                    writer.append(
                      MobUtils.createMobRefCell(mobCell, fileName, this.mobStore.getRefCellTags()));
                    // Update total size of the output (we do not take into account
                    // file compression yet)
                    long len = mobFileWriter.getPos();
                    if (len > maxMobFileSize) {
                      LOG.debug("Closing output MOB File, length={} file={}, store={}", len,
                        mobFileWriter.getPath().getName(), getStoreInfo());
                      commitOrAbortMobWriter(mobFileWriter, fd.maxSeqId, mobCells, major);
                      mobFileWriter = newMobWriter(fd, major);
                      fileName = Bytes.toBytes(mobFileWriter.getPath().getName());
                      mobCells = 0;
                    }
                  } else {
                    // We leave large MOB file as is (is not compacted),
                    // then we update set of MOB file references
                    // and append mob cell directly to the store's writer
                    Optional<TableName> refTable = MobUtils.getTableName(c);
                    if (refTable.isPresent()) {
                      mobRefSet.get().put(refTable.get(), fName);
                      writer.append(c);
                    } else {
                      throw new IOException(String.format("MOB cell did not contain a tablename " +
                          "tag. should not be possible. see ref guide on mob troubleshooting. " +
                          "store=%s cell=%s", getStoreInfo(), c));
                    }
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
                  long len = mobFileWriter.getPos();
                  if (len > maxMobFileSize) {
                    commitOrAbortMobWriter(mobFileWriter, fd.maxSeqId, mobCells, major);
                    mobFileWriter = newMobWriter(fd, major);
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
              String errMsg = String.format("Corrupted MOB reference: %s", c.toString());
              throw new IOException(errMsg);
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
              long len = mobFileWriter.getPos();
              if (len > maxMobFileSize) {
                commitOrAbortMobWriter(mobFileWriter, fd.maxSeqId, mobCells, major);
                mobFileWriter = newMobWriter(fd, major);
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
    } catch (IOException t) {
      String msg = "Mob compaction failed for region: " +
        store.getRegionInfo().getEncodedName();
      throw new IOException(msg, t);
    } finally {
      // Clone last cell in the final because writer will append last cell when committing. If
      // don't clone here and once the scanner get closed, then the memory of last cell will be
      // released. (HBASE-22582)
      ((ShipperListener) writer).beforeShipped();
      throughputController.finish(compactionName);
      if (!finished && mobFileWriter != null) {
        // Remove all MOB references because compaction failed
        clearThreadLocals();
        // Abort writer
        LOG.debug("Aborting writer for {} because of a compaction failure, Store {}",
          mobFileWriter.getPath(), getStoreInfo());
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

  protected String getStoreInfo() {
    return String.format("[table=%s family=%s region=%s]", store.getTableName().getNameAsString(),
      store.getColumnFamilyName(), store.getRegionInfo().getEncodedName()) ;
  }

  private void clearThreadLocals() {
    mobRefSet.get().clear();
    HashMap<String, Long> map = mobLengthMap.get();
    if (map != null) {
      map.clear();
    }
  }

  private StoreFileWriter newMobWriter(FileDetails fd, boolean major)
      throws IOException {
    try {
      StoreFileWriter mobFileWriter = mobStore.createWriterInTmp(new Date(fd.latestPutTs),
        fd.maxKeyCount, major ? majorCompactionCompression : minorCompactionCompression,
        store.getRegionInfo().getStartKey(), true);
      LOG.debug("New MOB writer created={} store={}", mobFileWriter.getPath().getName(),
        getStoreInfo());
      // Add reference we get for compact MOB
      mobRefSet.get().put(store.getTableName(), mobFileWriter.getPath().getName());
      return mobFileWriter;
    } catch (IOException e) {
      // Bailing out
      throw new IOException(String.format("Failed to create mob writer, store=%s",
        getStoreInfo()), e);
    }
  }

  private void commitOrAbortMobWriter(StoreFileWriter mobFileWriter, long maxSeqId, long mobCells,
      boolean major) throws IOException {
    // Commit or abort major mob writer
    // If IOException happens during below operation, some
    // MOB files can be committed partially, but corresponding
    // store file won't be committed, therefore these MOB files
    // become orphans and will be deleted during next MOB cleaning chore cycle

    if (mobFileWriter != null) {
      LOG.debug("Commit or abort size={} mobCells={} major={} file={}, store={}",
        mobFileWriter.getPos(), mobCells, major, mobFileWriter.getPath().getName(),
        getStoreInfo());
      Path path =
          MobUtils.getMobFamilyPath(conf, store.getTableName(), store.getColumnFamilyName());
      if (mobCells > 0) {
        // If the mob file is not empty, commit it.
        mobFileWriter.appendMetadata(maxSeqId, major, mobCells);
        mobFileWriter.close();
        mobStore.commitFile(mobFileWriter.getPath(), path);
      } else {
        // If the mob file is empty, delete it instead of committing.
        LOG.debug("Aborting writer for {} because there are no MOB cells, store={}",
          mobFileWriter.getPath(), getStoreInfo());
        // Remove MOB file from reference set
        mobRefSet.get().remove(store.getTableName(), mobFileWriter.getPath().getName());
        abortWriter(mobFileWriter);
      }
    } else {
      LOG.debug("Mob file writer is null, skipping commit/abort, store=",
        getStoreInfo());
    }
  }

  @Override
  protected List<Path> commitWriter(StoreFileWriter writer, FileDetails fd,
      CompactionRequestImpl request) throws IOException {
    List<Path> newFiles = Lists.newArrayList(writer.getPath());
    writer.appendMetadata(fd.maxSeqId, request.isAllFiles(), request.getFiles());
    writer.appendMobMetadata(mobRefSet.get());
    writer.close();
    clearThreadLocals();
    return newFiles;
  }

}
