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
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.regionserver.BloomType;
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
   * Contains list of MOB files for compaction if 
   * generational compaction is enabled.
   */
  static ThreadLocal<List<CompactionSelection>> compSelections =
      new ThreadLocal<List<CompactionSelection>>();

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
    // Check if generational MOB compaction
    compSelections.set(null);
    if (conf.get(MobConstants.MOB_COMPACTION_TYPE_KEY, MobConstants.DEFAULT_MOB_COMPACTION_TYPE)
        .equals(MobConstants.GENERATIONAL_MOB_COMPACTION_TYPE)) {
      if (request.isMajor() && request.getPriority() == HStore.PRIORITY_USER) {
        // Compact MOBs
        List<Path> mobFiles = getReferencedMobFiles(request.getFiles());
        if (mobFiles.size() > 0) {
          Generations gens = Generations.build(mobFiles, conf);
          List<CompactionSelection> list = gens.getCompactionSelections();
          if (list.size() > 0) {
            compSelections.set(list);
          }
        }
      }
    }
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
    boolean generationalMob = conf.get(MobConstants.MOB_COMPACTION_TYPE_KEY,
      MobConstants.DEFAULT_MOB_COMPACTION_TYPE)
        .equals(MobConstants.GENERATIONAL_MOB_COMPACTION_TYPE);
    if (generationalMob && compSelections.get() == null) {
      LOG.warn("MOB compaction aborted, reason: generational compaction is enabled, "+
              "but compaction selection was empty.");
      return true;
    }
    OutputMobWriters mobWriters = null;

    if (compactMOBs && generationalMob) {
      List<CompactionSelection> sel = compSelections.get();
      if (sel != null && sel.size() > 0) {
        // Create output writers for compaction selections
        mobWriters = new OutputMobWriters(sel);
        int numWriters = mobWriters.getNumberOfWriters();
        List<StoreFileWriter> writers = new ArrayList<StoreFileWriter>();
        for (int i=0; i < numWriters; i++) {
          StoreFileWriter sfw = mobStore.createWriterInTmp(new Date(fd.latestPutTs), fd.maxKeyCount,
            compactionCompression, store.getRegionInfo().getStartKey(), true);
          writers.add(sfw);
        }
        mobWriters.initOutputWriters(writers);
      }
    }

    boolean discardMobMiss =
        conf.getBoolean(MobConstants.MOB_DISCARD_MISS_KEY, MobConstants.DEFAULT_MOB_DISCARD_MISS);
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
      try {
        mobFileWriter = mobStore.createWriterInTmp(new Date(fd.latestPutTs), fd.maxKeyCount,
          compactionCompression, store.getRegionInfo().getStartKey(), true);
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
        mobRefSet.get().add(mobFileWriter.getPath().getName());
      }
      do {
        hasMore = scanner.next(cells, scannerContext);
        if (LOG.isDebugEnabled()) {
          now = EnvironmentEdgeManager.currentTime();
        }
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
                  LOG.error("Missing MOB cell: file=" + pp + " not found");
                  continue;
                } else {
                  throw fnfe;
                }
              }

              if (discardMobMiss && mobCell.getValueLength() == 0) {
                LOG.error("Missing MOB cell value: file=" + pp + " cell=" + mobCell);
                continue;
              }

              if (mobCell.getValueLength() > mobSizeThreshold) {
                // put the mob data back to the store file
                PrivateCellUtil.setSequenceId(mobCell, c.getSequenceId());
                if (generationalMob) {
                  //TODO: verify fName
                  StoreFileWriter stw = mobWriters.getOutputWriterForInputFile(fName);
                  if (stw != null) {
                    stw.append(mobCell);
                    mobWriters.incrementMobCountForOutputWriter(stw, 1);
                  } // else leave mob cell in a MOB file which is not in compaction selections
                } else {
                  mobFileWriter.append(mobCell);
                  mobCells++;
                }
                writer.append(MobUtils.createMobRefCell(mobCell, fileName,
                  this.mobStore.getRefCellTags()));
                cellsCountCompactedFromMob++;
                cellsSizeCompactedFromMob += mobCell.getValueLength();
              } else {

                // If MOB value is less than threshold, append it directly to a store file
                PrivateCellUtil.setSequenceId(mobCell, c.getSequenceId());
                writer.append(mobCell);
              }

            } else {
              // Not a MOB reference cell
              int size = c.getValueLength();
              if (size > mobSizeThreshold) {
                // This MOB cell comes from a regular store file
                // therefore we store it in original mob output
                mobFileWriter.append(c);
                writer
                    .append(MobUtils.createMobRefCell(c, fileName, this.mobStore.getRefCellTags()));
                mobCells++;
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
                writer.append(c);
                // Add MOB reference to a set
                mobRefSet.get().add(MobUtils.getMobFileName(c));
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
                  writer.append(c);
                  // Add MOB reference to a set
                  mobRefSet.get().add(MobUtils.getMobFileName(c));
                }
              }
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
            // Add ref we get for compact MOB case
            mobRefSet.get().add(mobFileWriter.getPath().getName());
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
        //Check if other writers exist
        if (mobWriters != null) {
          for(StoreFileWriter w: mobWriters.getOutputWriters()) {
            abortWriter(w);
          }
        }
      }
    }
    // Commit or abort major mob writer
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
    // Commit or abort generational writers
    if (mobWriters != null) {
      for (StoreFileWriter w: mobWriters.getOutputWriters()) {
        Long mobs = mobWriters.getMobCountForOutputWriter(w);
        if (mobs != null && mobs > 0) {
          mobRefSet.get().add(w.getPath().getName());
          w.appendMetadata(fd.maxSeqId, major, mobs);
          w.close();
          mobStore.commitFile(w.getPath(), path);
        } else {
          abortWriter(w);
        }
      }
    }
    mobStore.updateCellsCountCompactedFromMob(cellsCountCompactedFromMob);
    mobStore.updateCellsCountCompactedToMob(cellsCountCompactedToMob);
    mobStore.updateCellsSizeCompactedFromMob(cellsSizeCompactedFromMob);
    mobStore.updateCellsSizeCompactedToMob(cellsSizeCompactedToMob);
    progress.complete();
    return true;
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

  private List<Path> getReferencedMobFiles(Collection<HStoreFile> storeFiles) {
    Path mobDir = MobUtils.getMobFamilyPath(conf, store.getTableName(),
      store.getColumnFamilyName());
    Set<String> mobSet = new HashSet<String>();
    for (HStoreFile sf: storeFiles) {
      byte[] value = sf.getMetadataValue(HStoreFile.MOB_FILE_REFS);
      if (value != null && value.length > 1) {
        String s = Bytes.toString(value);
        String[] all = s.split(",");
        Collections.addAll(mobSet, all);
      }
    }
    List<Path> retList = new ArrayList<Path>();
    for(String name: mobSet) {
      retList.add(new Path(mobDir, name));
    }
    return retList;
  }
}

final class FileSelection implements Comparable<FileSelection> {

  public final static String NULL_REGION = "";
  private Path path;
  private long earliestTs;
  private Configuration conf;

  public FileSelection(Path path, Configuration conf) throws IOException {
    this.path = path;
    this.conf = conf;
    readEarliestTimestamp();
  }

  public  String getEncodedRegionName() {
    String fileName = path.getName();
    String[] parts = fileName.split("_");
    if (parts.length == 2) {
      return parts[1];
    } else {
      return NULL_REGION;
    }
  }

  public Path getPath() {
    return path;
  }

  public long getEarliestTimestamp() {
    return earliestTs;
  }

  private void readEarliestTimestamp() throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    HStoreFile sf = new HStoreFile(fs, path, conf, CacheConfig.DISABLED,
      BloomType.NONE, true);
    sf.initReader();
    byte[] tsData = sf.getMetadataValue(HStoreFile.EARLIEST_PUT_TS);
    if (tsData != null) {
      this.earliestTs = Bytes.toLong(tsData);
    }
    sf.closeStoreFile(true);
  }

  @Override
  public int compareTo(FileSelection o) {
    if (this.earliestTs > o.earliestTs) {
      return +1;
    } else if (this.earliestTs == o.earliestTs) {
      return 0;
    } else {
      return -1;
    }
  }

}

final class Generations {

  private List<Generation> generations;
  private Configuration conf;

  private Generations(List<Generation> gens, Configuration conf) {
    this.generations = gens;
    this.conf = conf;
  }

  List<CompactionSelection> getCompactionSelections() throws IOException {
    int maxTotalFiles = this.conf.getInt(MobConstants.MOB_COMPACTION_MAX_TOTAL_FILES_KEY,
                                         MobConstants.DEFAULT_MOB_COMPACTION_MAX_TOTAL_FILES);
    int currentTotal = 0;
    List<CompactionSelection> list = new ArrayList<CompactionSelection>();

    for (Generation g: generations) {
      List<CompactionSelection> sel = g.getCompactionSelections(conf);
      int size = getSize(sel);
      if ((currentTotal + size > maxTotalFiles) && currentTotal > 0) {
        break;
      } else {
        currentTotal += size;
        list.addAll(sel);
      }
    }
    return list;
  }

  private int getSize(List<CompactionSelection> sel) {
    int size = 0;
    for(CompactionSelection cs: sel) {
      size += cs.size();
    }
    return size;
  }

  static Generations build(List<Path> files, Configuration conf) throws IOException {
    Map <String, ArrayList<FileSelection>> map = new HashMap<String, ArrayList<FileSelection>>();
    for(Path p: files) {
      String key = getRegionNameFromFileName(p.getName());
      ArrayList<FileSelection> list = map.get(key);
      if (list == null) {
        list = new ArrayList<FileSelection>();
        map.put(key, list);
      }
      list.add(new FileSelection(p, conf));
    }

    List<Generation> gens = new ArrayList<Generation>();
    for (Map.Entry<String, ArrayList<FileSelection>> entry: map.entrySet()) {
      String key = entry.getKey();
      Generation g = new Generation(key);
      List<FileSelection> selFiles = map.get(key);
      for(FileSelection fs: selFiles) {
        g.addFile(fs);
      }
      gens.add(g);
    }
    // Sort all generation files one-by-one
    for(Generation gg: gens) {
      gg.sortFiles();
    }
    // Sort generations
    Collections.sort(gens);
    return new Generations(gens, conf);
  }

  static String getRegionNameFromFileName(String name) {
    int index = name.lastIndexOf("_");
    if (index < 0) {
      return Generation.GEN0;
    }
    return name.substring(index+1);
  }
}

final class Generation implements Comparable<Generation> {

  static final String GEN0 ="GEN0";
  private String regionName;
  private long earliestTs = Long.MAX_VALUE;
  private List<FileSelection> files = new ArrayList<>();
  List<CompactionSelection> compSelections;

  public Generation(String name) {
    this.regionName = name;
  }

  @SuppressWarnings("deprecation")
  public List<CompactionSelection> getCompactionSelections(Configuration conf) throws IOException {


    int minFiles = conf.getInt(MobConstants.MOB_COMPACTION_MIN_FILES_KEY,
                                MobConstants.DEFAULT_MOB_COMPACTION_MIN_FILES);
    int maxFiles = conf.getInt(MobConstants.MOB_COMPACTION_MAX_FILES_KEY,
                                MobConstants.DEFAULT_MOB_COMPACTION_MAX_FILES);
    long maxSelectionSize = conf.getLong(MobConstants.MOB_COMPACTION_MAX_SELECTION_SIZE_KEY,
                    MobConstants.DEFAULT_MOB_COMPACTION_MAX_SELECTION_SIZE);
    // Now it is ordered from oldest to newest ones
    List<FileSelection> rfiles = Lists.reverse(files);
    List<CompactionSelection> retList = new ArrayList<CompactionSelection>();
    FileSystem fs = rfiles.get(0).getPath().getFileSystem(conf);
    int off = 0;
    while (off < rfiles.size()) {
      if (fs.getLength(rfiles.get(off).getPath()) >= maxSelectionSize) {
        off++; continue;
      }
      long selSize = 0;
      int limit = Math.min(off + maxFiles, rfiles.size());
      int start = off;
      List<FileSelection> sel = new ArrayList<FileSelection>();
      for (; off < limit; off++) {
        Path p = rfiles.get(off).getPath();
        long fSize = fs.getLength(p);
        if (selSize + fSize < maxSelectionSize) {
          selSize+= fSize;
          sel.add(new FileSelection(p, conf));
        } else {
          if (sel.size() < minFiles) {
            // discard
            sel.clear();
            // advance by 1
            off = start +1;
          } else {
            // we have new selection
            CompactionSelection cs = new CompactionSelection(sel);
            retList.add(cs);
            off++;
          }
          break; // continue outer loop
        }
      }
    }
    return retList;
  }

  public boolean addFile(FileSelection f) {
    if (f.getEncodedRegionName().equals(regionName)) {
      files.add(f);
      if (f.getEarliestTimestamp() < earliestTs) {
        earliestTs = f.getEarliestTimestamp();
      }
      return true;
    } else {
      return false;
    }
  }

  public void sortFiles() {
    Collections.sort(files);
  }

  public List<FileSelection> getFiles() {
    return files;
  }

  public String getEncodedRegionName() {
    return regionName;
  }

  public long getEarliestTimestamp() {
    return earliestTs;
  }

  @Override
  public int compareTo(Generation o) {
    if (this.earliestTs > o.earliestTs) {
      return +1;
    } else if (this.earliestTs == o.earliestTs) {
      return 0;
    } else {
      return -1;
    }
  }
}

final class CompactionSelection {
  private static AtomicLong idGen = new AtomicLong();
  private List<FileSelection> files;
  private long id;

  public CompactionSelection(List<FileSelection> files) {
    this.files = files;
    this.id = idGen.getAndIncrement();
  }

  public List<FileSelection> getFiles() {
    return files;
  }

  public long getId() {
    return id;
  }

  int size() {
    return files.size();
  }
}

final class OutputMobWriters {

  /*
   * Input MOB file name -> output file writer
   */
  private Map<String, StoreFileWriter> writerMap = new HashMap<String, StoreFileWriter>();
  /*
   * Output file name -> MOB counter
   */
  private Map<String, Long> mapMobCounts = new HashMap<String, Long>();
  /*
   * List of compaction selections
   */
  private List<CompactionSelection> compSelections;

  public OutputMobWriters(List<CompactionSelection> compSelections) {
    this.compSelections = compSelections;
  }

  int getNumberOfWriters() {
    return compSelections.size();
  }

  StoreFileWriter getWriterForFile(String fileName) {
    return writerMap.get(fileName);
  }

  void initOutputWriters(List<StoreFileWriter> writers) {
    for (int i = 0; i < writers.size(); i++) {
      StoreFileWriter sw = writers.get(i);
      mapMobCounts.put(sw.getPath().getName(), 0L);
      CompactionSelection cs = compSelections.get(i);
      for (FileSelection fs: cs.getFiles()) {
        writerMap.put(fs.getPath().getName(), sw);
      }
    }
  }

  Collection<StoreFileWriter> getOutputWriters() {
    return writerMap.values();
  }

  StoreFileWriter getOutputWriterForInputFile(String name) {
    return writerMap.get(name);
  }

  long getMobCountForOutputWriter(StoreFileWriter writer) {
    return mapMobCounts.get(writer.getPath().getName());
  }

  void incrementMobCountForOutputWriter(StoreFileWriter writer, int val) {
    String key = writer.getPath().getName();
    mapMobCounts.compute(key, (k,v) -> v == null? val: v + val);
  }
}
