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
package org.apache.hadoop.hbase.mob.compactions;

import static org.apache.hadoop.hbase.regionserver.HStoreFile.BULKLOAD_TIME_KEY;
import static org.apache.hadoop.hbase.regionserver.HStoreFile.MOB_CELLS_COUNT;
import static org.apache.hadoop.hbase.regionserver.HStoreFile.SKIP_RESET_SEQ_ID;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ArrayBackedTag;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.TagType;
import org.apache.hadoop.hbase.TagUtil;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.MobCompactPartitionPolicy;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFile.Reader;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobFileName;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.mob.compactions.MobCompactionRequest.CompactionType;
import org.apache.hadoop.hbase.mob.compactions.PartitionedMobCompactionRequest.CompactionDelPartition;
import org.apache.hadoop.hbase.mob.compactions.PartitionedMobCompactionRequest.CompactionDelPartitionId;
import org.apache.hadoop.hbase.mob.compactions.PartitionedMobCompactionRequest.CompactionPartition;
import org.apache.hadoop.hbase.mob.compactions.PartitionedMobCompactionRequest.CompactionPartitionId;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.ScanInfo;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;
import org.apache.hadoop.hbase.regionserver.StoreFileWriter;
import org.apache.hadoop.hbase.regionserver.StoreScanner;
import org.apache.hadoop.hbase.security.EncryptionUtil;
import org.apache.hadoop.hbase.tool.BulkLoadHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * An implementation of {@link MobCompactor} that compacts the mob files in partitions.
 */
@InterfaceAudience.Private
public class PartitionedMobCompactor extends MobCompactor {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionedMobCompactor.class);
  protected long mergeableSize;
  protected int delFileMaxCount;
  /** The number of files compacted in a batch */
  protected int compactionBatchSize;
  protected int compactionKVMax;

  private final Path tempPath;
  private final Path bulkloadPath;
  private final CacheConfig compactionCacheConfig;
  private final byte[] refCellTags;
  private Encryption.Context cryptoContext = Encryption.Context.NONE;

  public PartitionedMobCompactor(Configuration conf, FileSystem fs, TableName tableName,
                                 ColumnFamilyDescriptor column, ExecutorService pool) throws IOException {
    super(conf, fs, tableName, column, pool);
    mergeableSize = conf.getLong(MobConstants.MOB_COMPACTION_MERGEABLE_THRESHOLD,
      MobConstants.DEFAULT_MOB_COMPACTION_MERGEABLE_THRESHOLD);
    delFileMaxCount = conf.getInt(MobConstants.MOB_DELFILE_MAX_COUNT,
      MobConstants.DEFAULT_MOB_DELFILE_MAX_COUNT);
    // default is 100
    compactionBatchSize = conf.getInt(MobConstants.MOB_COMPACTION_BATCH_SIZE,
      MobConstants.DEFAULT_MOB_COMPACTION_BATCH_SIZE);
    tempPath = new Path(MobUtils.getMobHome(conf), MobConstants.TEMP_DIR_NAME);
    bulkloadPath = new Path(tempPath, new Path(MobConstants.BULKLOAD_DIR_NAME, new Path(
      tableName.getNamespaceAsString(), tableName.getQualifierAsString())));
    compactionKVMax = this.conf.getInt(HConstants.COMPACTION_KV_MAX,
      HConstants.COMPACTION_KV_MAX_DEFAULT);
    Configuration copyOfConf = new Configuration(conf);
    copyOfConf.setFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0f);
    compactionCacheConfig = new CacheConfig(copyOfConf);
    List<Tag> tags = new ArrayList<>(2);
    tags.add(MobConstants.MOB_REF_TAG);
    Tag tableNameTag = new ArrayBackedTag(TagType.MOB_TABLE_NAME_TAG_TYPE, tableName.getName());
    tags.add(tableNameTag);
    this.refCellTags = TagUtil.fromList(tags);
    cryptoContext = EncryptionUtil.createEncryptionContext(copyOfConf, column);
  }

  @Override
  public List<Path> compact(List<FileStatus> files, boolean allFiles) throws IOException {
    if (files == null || files.isEmpty()) {
      LOG.info("No candidate mob files");
      return null;
    }
    LOG.info("is allFiles: " + allFiles);

    // find the files to compact.
    PartitionedMobCompactionRequest request = select(files, allFiles);
    // compact the files.
    return performCompaction(request);
  }

  /**
   * Selects the compacted mob/del files.
   * Iterates the candidates to find out all the del files and small mob files.
   * @param candidates All the candidates.
   * @param allFiles Whether add all mob files into the compaction.
   * @return A compaction request.
   * @throws IOException if IO failure is encountered
   */
  protected PartitionedMobCompactionRequest select(List<FileStatus> candidates,
    boolean allFiles) throws IOException {
    final Map<CompactionPartitionId, CompactionPartition> filesToCompact = new HashMap<>();
    final CompactionPartitionId id = new CompactionPartitionId();
    final NavigableMap<CompactionDelPartitionId, CompactionDelPartition> delFilesToCompact = new TreeMap<>();
    final CompactionDelPartitionId delId = new CompactionDelPartitionId();
    final ArrayList<CompactionDelPartition> allDelPartitions = new ArrayList<>();
    int selectedFileCount = 0;
    int irrelevantFileCount = 0;
    int totalDelFiles = 0;
    MobCompactPartitionPolicy policy = column.getMobCompactPartitionPolicy();

    Calendar calendar =  Calendar.getInstance();
    Date currentDate = new Date();
    Date firstDayOfCurrentMonth = null;
    Date firstDayOfCurrentWeek = null;

    if (policy == MobCompactPartitionPolicy.MONTHLY) {
      firstDayOfCurrentMonth = MobUtils.getFirstDayOfMonth(calendar, currentDate);
      firstDayOfCurrentWeek = MobUtils.getFirstDayOfWeek(calendar, currentDate);
    } else if (policy == MobCompactPartitionPolicy.WEEKLY) {
      firstDayOfCurrentWeek = MobUtils.getFirstDayOfWeek(calendar, currentDate);
    }

    // We check if there is any del files so the logic can be optimized for the following processing
    // First step is to check if there is any delete files. If there is any delete files,
    // For each Partition, it needs to read its startKey and endKey from files.
    // If there is no delete file, there is no need to read startKey and endKey from files, this
    // is an optimization.
    boolean withDelFiles = false;
    for (FileStatus file : candidates) {
      if (!file.isFile()) {
        continue;
      }
      // group the del files and small files.
      FileStatus linkedFile = file;
      if (HFileLink.isHFileLink(file.getPath())) {
        HFileLink link = HFileLink.buildFromHFileLinkPattern(conf, file.getPath());
        linkedFile = getLinkedFileStatus(link);
        if (linkedFile == null) {
          continue;
        }
      }
      if (StoreFileInfo.isDelFile(linkedFile.getPath())) {
        withDelFiles = true;
        break;
      }
    }

    for (FileStatus file : candidates) {
      if (!file.isFile()) {
        irrelevantFileCount++;
        continue;
      }
      // group the del files and small files.
      FileStatus linkedFile = file;
      if (HFileLink.isHFileLink(file.getPath())) {
        HFileLink link = HFileLink.buildFromHFileLinkPattern(conf, file.getPath());
        linkedFile = getLinkedFileStatus(link);
        if (linkedFile == null) {
          // If the linked file cannot be found, regard it as an irrelevantFileCount file
          irrelevantFileCount++;
          continue;
        }
      }
      if (withDelFiles && StoreFileInfo.isDelFile(linkedFile.getPath())) {
        // File in the Del Partition List

        // Get delId from the file
        try (Reader reader = HFile.createReader(fs, linkedFile.getPath(), conf)) {
          delId.setStartKey(reader.getFirstRowKey().get());
          delId.setEndKey(reader.getLastRowKey().get());
        }
        CompactionDelPartition delPartition = delFilesToCompact.get(delId);
        if (delPartition == null) {
          CompactionDelPartitionId newDelId =
              new CompactionDelPartitionId(delId.getStartKey(), delId.getEndKey());
          delPartition = new CompactionDelPartition(newDelId);
          delFilesToCompact.put(newDelId, delPartition);
        }
        delPartition.addDelFile(file);
        totalDelFiles ++;
      } else {
        String fileName = linkedFile.getPath().getName();
        String date = MobFileName.getDateFromName(fileName);
        boolean skipCompaction = MobUtils
            .fillPartitionId(id, firstDayOfCurrentMonth, firstDayOfCurrentWeek, date, policy,
                calendar, mergeableSize);
        if (allFiles || (!skipCompaction && (linkedFile.getLen() < id.getThreshold()))) {
          // add all files if allFiles is true,
          // otherwise add the small files to the merge pool
          // filter out files which are not supposed to be compacted with the
          // current policy

          id.setStartKey(MobFileName.getStartKeyFromName(fileName));
          CompactionPartition compactionPartition = filesToCompact.get(id);
          if (compactionPartition == null) {
            CompactionPartitionId newId = new CompactionPartitionId(id.getStartKey(), id.getDate());
            compactionPartition = new CompactionPartition(newId);
            compactionPartition.addFile(file);
            filesToCompact.put(newId, compactionPartition);
            newId.updateLatestDate(date);
          } else {
            compactionPartition.addFile(file);
            compactionPartition.getPartitionId().updateLatestDate(date);
          }

          if (withDelFiles) {
            // get startKey and endKey from the file and update partition
            // TODO: is it possible to skip read of most hfiles?
            try (Reader reader = HFile.createReader(fs, linkedFile.getPath(), conf)) {
              compactionPartition.setStartKey(reader.getFirstRowKey().get());
              compactionPartition.setEndKey(reader.getLastRowKey().get());
            }
          }

          selectedFileCount++;
        }
      }
    }

    /*
     * Merge del files so there are only non-overlapped del file lists
     */
    for(Map.Entry<CompactionDelPartitionId, CompactionDelPartition> entry : delFilesToCompact.entrySet()) {
      if (allDelPartitions.size() > 0) {
        // check if the current key range overlaps the previous one
        CompactionDelPartition prev = allDelPartitions.get(allDelPartitions.size() - 1);
        if (Bytes.compareTo(prev.getId().getEndKey(), entry.getKey().getStartKey()) >= 0) {
          // merge them together
          prev.getId().setEndKey(entry.getValue().getId().getEndKey());
          prev.addDelFileList(entry.getValue().listDelFiles());

        } else {
          allDelPartitions.add(entry.getValue());
        }
      } else {
        allDelPartitions.add(entry.getValue());
      }
    }

    PartitionedMobCompactionRequest request = new PartitionedMobCompactionRequest(
      filesToCompact.values(), allDelPartitions);
    if (candidates.size() == (totalDelFiles + selectedFileCount + irrelevantFileCount)) {
      // all the files are selected
      request.setCompactionType(CompactionType.ALL_FILES);
    }
    LOG.info("The compaction type is " + request.getCompactionType() + ", the request has "
      + totalDelFiles + " del files, " + selectedFileCount + " selected files, and "
      + irrelevantFileCount + " irrelevant files");
    return request;
  }

  /**
   * Performs the compaction on the selected files.
   * <ol>
   * <li>Compacts the del files.</li>
   * <li>Compacts the selected small mob files and all the del files.</li>
   * <li>If all the candidates are selected, delete the del files.</li>
   * </ol>
   * @param request The compaction request.
   * @return The paths of new mob files generated in the compaction.
   * @throws IOException if IO failure is encountered
   */
  protected List<Path> performCompaction(PartitionedMobCompactionRequest request)
    throws IOException {

    // merge the del files, it is per del partition
    for (CompactionDelPartition delPartition : request.getDelPartitions()) {
      if (delPartition.getDelFileCount() <= 1) continue;
      List<Path> newDelPaths = compactDelFiles(request, delPartition.listDelFiles());
      delPartition.cleanDelFiles();
      delPartition.addDelFileList(newDelPaths);
    }

    List<Path> paths = null;
    int totalDelFileCount = 0;
    try {
      for (CompactionDelPartition delPartition : request.getDelPartitions()) {
        for (Path newDelPath : delPartition.listDelFiles()) {
          HStoreFile sf =
              new HStoreFile(fs, newDelPath, conf, compactionCacheConfig, BloomType.NONE, true);
          // pre-create reader of a del file to avoid race condition when opening the reader in each
          // partition.
          sf.initReader();
          delPartition.addStoreFile(sf);
          totalDelFileCount++;
        }
      }
      LOG.info("After merging, there are " + totalDelFileCount + " del files");
      // compact the mob files by partitions.
      paths = compactMobFiles(request);
      LOG.info("After compaction, there are " + paths.size() + " mob files");
    } finally {
      for (CompactionDelPartition delPartition : request.getDelPartitions()) {
        closeStoreFileReaders(delPartition.getStoreFiles());
      }
    }

    // archive the del files if all the mob files are selected.
    if (request.type == CompactionType.ALL_FILES && !request.getDelPartitions().isEmpty()) {
      LOG.info(
          "After a mob compaction with all files selected, archiving the del files ");
      for (CompactionDelPartition delPartition : request.getDelPartitions()) {
        LOG.info(Objects.toString(delPartition.listDelFiles()));
        try {
          MobUtils.removeMobFiles(conf, fs, tableName, mobTableDir, column.getName(),
            delPartition.getStoreFiles());
        } catch (IOException e) {
          LOG.error("Failed to archive the del files " + delPartition.getStoreFiles(), e);
        }
      }
    }
    return paths;
  }

  static class DelPartitionComparator implements Comparator<CompactionDelPartition> {
    private boolean compareStartKey;

    DelPartitionComparator(boolean compareStartKey) {
      this.compareStartKey = compareStartKey;
    }

    public boolean getCompareStartKey() {
      return this.compareStartKey;
    }

    public void setCompareStartKey(final boolean compareStartKey) {
      this.compareStartKey = compareStartKey;
    }

    @Override
    public int compare(CompactionDelPartition o1, CompactionDelPartition o2) {

      if (compareStartKey) {
        return Bytes.compareTo(o1.getId().getStartKey(), o2.getId().getStartKey());
      } else {
        return Bytes.compareTo(o1.getId().getEndKey(), o2.getId().getEndKey());
      }
    }
  }

  @VisibleForTesting
  List<HStoreFile> getListOfDelFilesForPartition(final CompactionPartition partition,
      final List<CompactionDelPartition> delPartitions) {
    // Binary search for startKey and endKey

    List<HStoreFile> result = new ArrayList<>();

    DelPartitionComparator comparator = new DelPartitionComparator(false);
    CompactionDelPartitionId id = new CompactionDelPartitionId(null, partition.getStartKey());
    CompactionDelPartition target = new CompactionDelPartition(id);
    int start = Collections.binarySearch(delPartitions, target, comparator);

    // Get the start index for partition
    if (start < 0) {
      // Calculate the insert point
      start = (start + 1) * (-1);
      if (start == delPartitions.size()) {
        // no overlap
        return result;
      } else {
        // Check another case which has no overlap
        if (Bytes.compareTo(partition.getEndKey(), delPartitions.get(start).getId().getStartKey()) < 0) {
          return result;
        }
      }
    }

    // Search for end index for the partition
    comparator.setCompareStartKey(true);
    id.setStartKey(partition.getEndKey());
    int end = Collections.binarySearch(delPartitions, target, comparator);

    if (end < 0) {
      end = (end + 1) * (-1);
      if (end == 0) {
        return result;
      } else {
        --end;
        if (Bytes.compareTo(partition.getStartKey(), delPartitions.get(end).getId().getEndKey()) > 0) {
          return result;
        }
      }
    }

    for (int i = start; i <= end; ++i) {
        result.addAll(delPartitions.get(i).getStoreFiles());
    }

    return result;
  }

  /**
   * Compacts the selected small mob files and all the del files.
   * @param request The compaction request.
   * @return The paths of new mob files after compactions.
   * @throws IOException if IO failure is encountered
   */
  protected List<Path> compactMobFiles(final PartitionedMobCompactionRequest request)
      throws IOException {
    Collection<CompactionPartition> partitions = request.compactionPartitions;
    if (partitions == null || partitions.isEmpty()) {
      LOG.info("No partitions of mob files");
      return Collections.emptyList();
    }
    List<Path> paths = new ArrayList<>();
    final Connection c = ConnectionFactory.createConnection(conf);
    final Table table = c.getTable(tableName);

    try {
      Map<CompactionPartitionId, Future<List<Path>>> results = new HashMap<>();
      // compact the mob files by partitions in parallel.
      for (final CompactionPartition partition : partitions) {

        // How to efficiently come up a list of delFiles for one partition?
        // Search the delPartitions and collect all the delFiles for the partition
        // One optimization can do is that if there is no del file, we do not need to
        // come up with startKey/endKey.
        List<HStoreFile> delFiles = getListOfDelFilesForPartition(partition,
            request.getDelPartitions());

        results.put(partition.getPartitionId(), pool.submit(new Callable<List<Path>>() {
          @Override
          public List<Path> call() throws Exception {
            LOG.info("Compacting mob files for partition " + partition.getPartitionId());
            return compactMobFilePartition(request, partition, delFiles, c, table);
          }
        }));
      }
      // compact the partitions in parallel.
      List<CompactionPartitionId> failedPartitions = new ArrayList<>();
      for (Entry<CompactionPartitionId, Future<List<Path>>> result : results.entrySet()) {
        try {
          paths.addAll(result.getValue().get());
        } catch (Exception e) {
          // just log the error
          LOG.error("Failed to compact the partition " + result.getKey(), e);
          failedPartitions.add(result.getKey());
        }
      }
      if (!failedPartitions.isEmpty()) {
        // if any partition fails in the compaction, directly throw an exception.
        throw new IOException("Failed to compact the partitions " + failedPartitions);
      }
    } finally {
      try {
        table.close();
      } catch (IOException e) {
        LOG.error("Failed to close the Table", e);
      }
    }
    return paths;
  }

  /**
   * Compacts a partition of selected small mob files and all the del files.
   * @param request The compaction request.
   * @param partition A compaction partition.
   * @param delFiles The del files.
   * @param connection The connection to use.
   * @param table The current table.
   * @return The paths of new mob files after compactions.
   * @throws IOException if IO failure is encountered
   */
  private List<Path> compactMobFilePartition(PartitionedMobCompactionRequest request,
                                             CompactionPartition partition,
                                             List<HStoreFile> delFiles,
                                             Connection connection,
                                             Table table) throws IOException {
    if (MobUtils.isMobFileExpired(column, EnvironmentEdgeManager.currentTime(),
      partition.getPartitionId().getDate())) {
      // If the files in the partition are expired, do not compact them and directly
      // return an empty list.
      return Collections.emptyList();
    }
    List<Path> newFiles = new ArrayList<>();
    List<FileStatus> files = partition.listFiles();
    int offset = 0;
    Path bulkloadPathOfPartition = new Path(bulkloadPath, partition.getPartitionId().toString());
    Path bulkloadColumnPath = new Path(bulkloadPathOfPartition, column.getNameAsString());
    while (offset < files.size()) {
      int batch = compactionBatchSize;
      if (files.size() - offset < compactionBatchSize) {
        batch = files.size() - offset;
      }
      if (batch == 1 && delFiles.isEmpty()) {
        // only one file left and no del files, do not compact it,
        // and directly add it to the new files.
        newFiles.add(files.get(offset).getPath());
        offset++;
        continue;
      }
      // clean the bulkload directory to avoid loading old files.
      fs.delete(bulkloadPathOfPartition, true);
      // add the selected mob files and del files into filesToCompact
      List<HStoreFile> filesToCompact = new ArrayList<>();
      for (int i = offset; i < batch + offset; i++) {
        HStoreFile sf = new HStoreFile(fs, files.get(i).getPath(), conf, compactionCacheConfig,
            BloomType.NONE, true);
        filesToCompact.add(sf);
      }
      filesToCompact.addAll(delFiles);
      // compact the mob files in a batch.
      compactMobFilesInBatch(request, partition, connection, table, filesToCompact, batch,
        bulkloadPathOfPartition, bulkloadColumnPath, newFiles);
      // move to the next batch.
      offset += batch;
    }
    LOG.info("Compaction is finished. The number of mob files is changed from " + files.size()
      + " to " + newFiles.size());
    return newFiles;
  }

  /**
   * Closes the readers of store files.
   * @param storeFiles The store files to be closed.
   */
  private void closeStoreFileReaders(List<HStoreFile> storeFiles) {
    for (HStoreFile storeFile : storeFiles) {
      try {
        storeFile.closeStoreFile(true);
      } catch (IOException e) {
        LOG.warn("Failed to close the reader on store file " + storeFile.getPath(), e);
      }
    }
  }

  /**
   * Compacts a partition of selected small mob files and all the del files in a batch.
   * @param request The compaction request.
   * @param partition A compaction partition.
   * @param connection To use for transport
   * @param table The current table.
   * @param filesToCompact The files to be compacted.
   * @param batch The number of mob files to be compacted in a batch.
   * @param bulkloadPathOfPartition The directory where the bulkload column of the current
   *   partition is saved.
   * @param bulkloadColumnPath The directory where the bulkload files of current partition
   *   are saved.
   * @param newFiles The paths of new mob files after compactions.
   * @throws IOException if IO failure is encountered
   */
  private void compactMobFilesInBatch(PartitionedMobCompactionRequest request,
                                      CompactionPartition partition,
                                      Connection connection, Table table,
                                      List<HStoreFile> filesToCompact, int batch,
                                      Path bulkloadPathOfPartition, Path bulkloadColumnPath,
                                      List<Path> newFiles)
      throws IOException {
    // open scanner to the selected mob files and del files.
    StoreScanner scanner = createScanner(filesToCompact, ScanType.COMPACT_DROP_DELETES);
    // the mob files to be compacted, not include the del files.
    List<HStoreFile> mobFilesToCompact = filesToCompact.subList(0, batch);
    // Pair(maxSeqId, cellsCount)
    Pair<Long, Long> fileInfo = getFileInfo(mobFilesToCompact);
    // open writers for the mob files and new ref store files.
    StoreFileWriter writer = null;
    StoreFileWriter refFileWriter = null;
    Path filePath = null;
    long mobCells = 0;
    boolean cleanupTmpMobFile = false;
    boolean cleanupBulkloadDirOfPartition = false;
    boolean cleanupCommittedMobFile = false;
    boolean closeReaders= true;

    try {
      try {
        writer = MobUtils
            .createWriter(conf, fs, column, partition.getPartitionId().getLatestDate(), tempPath,
                Long.MAX_VALUE, column.getCompactionCompressionType(),
                partition.getPartitionId().getStartKey(), compactionCacheConfig, cryptoContext,
                true);
        cleanupTmpMobFile = true;
        filePath = writer.getPath();
        byte[] fileName = Bytes.toBytes(filePath.getName());
        // create a temp file and open a writer for it in the bulkloadPath
        refFileWriter = MobUtils.createRefFileWriter(conf, fs, column, bulkloadColumnPath,
            fileInfo.getSecond().longValue(), compactionCacheConfig, cryptoContext, true);
        cleanupBulkloadDirOfPartition = true;
        List<Cell> cells = new ArrayList<>();
        boolean hasMore;
        ScannerContext scannerContext =
            ScannerContext.newBuilder().setBatchLimit(compactionKVMax).build();
        do {
          hasMore = scanner.next(cells, scannerContext);
          for (Cell cell : cells) {
            // write the mob cell to the mob file.
            writer.append(cell);
            // write the new reference cell to the store file.
            Cell reference = MobUtils.createMobRefCell(cell, fileName, this.refCellTags);
            refFileWriter.append(reference);
            mobCells++;
          }
          cells.clear();
        } while (hasMore);
      } finally {
        // close the scanner.
        scanner.close();

        if (cleanupTmpMobFile) {
          // append metadata to the mob file, and close the mob file writer.
          closeMobFileWriter(writer, fileInfo.getFirst(), mobCells);
        }

        if (cleanupBulkloadDirOfPartition) {
          // append metadata and bulkload info to the ref mob file, and close the writer.
          closeRefFileWriter(refFileWriter, fileInfo.getFirst(), request.selectionTime);
        }
      }

      if (mobCells > 0) {
        // commit mob file
        MobUtils.commitFile(conf, fs, filePath, mobFamilyDir, compactionCacheConfig);
        cleanupTmpMobFile = false;
        cleanupCommittedMobFile = true;
        // bulkload the ref file
        bulkloadRefFile(table.getName(), bulkloadPathOfPartition, filePath.getName());
        cleanupCommittedMobFile = false;
        newFiles.add(new Path(mobFamilyDir, filePath.getName()));
      }

      // archive the old mob files, do not archive the del files.
      try {
        closeStoreFileReaders(mobFilesToCompact);
        closeReaders = false;
        MobUtils.removeMobFiles(conf, fs, tableName, mobTableDir, column.getName(), mobFilesToCompact);
      } catch (IOException e) {
        LOG.error("Failed to archive the files " + mobFilesToCompact, e);
      }
    } finally {
      if (closeReaders) {
        closeStoreFileReaders(mobFilesToCompact);
      }

      if (cleanupTmpMobFile) {
        deletePath(filePath);
      }

      if (cleanupBulkloadDirOfPartition) {
        // delete the bulkload files in bulkloadPath
        deletePath(bulkloadPathOfPartition);
      }

      if (cleanupCommittedMobFile) {
        deletePath(new Path(mobFamilyDir, filePath.getName()));
      }
    }
  }

  /**
   * Compacts the del files in batches which avoids opening too many files.
   * @param request The compaction request.
   * @param delFilePaths Del file paths to compact
   * @return The paths of new del files after merging or the original files if no merging
   *         is necessary.
   * @throws IOException if IO failure is encountered
   */
  protected List<Path> compactDelFiles(PartitionedMobCompactionRequest request,
    List<Path> delFilePaths) throws IOException {
    if (delFilePaths.size() <= delFileMaxCount) {
      return delFilePaths;
    }
    // when there are more del files than the number that is allowed, merge it firstly.
    int offset = 0;
    List<Path> paths = new ArrayList<>();
    while (offset < delFilePaths.size()) {
      // get the batch
      int batch = compactionBatchSize;
      if (delFilePaths.size() - offset < compactionBatchSize) {
        batch = delFilePaths.size() - offset;
      }
      List<HStoreFile> batchedDelFiles = new ArrayList<>();
      if (batch == 1) {
        // only one file left, do not compact it, directly add it to the new files.
        paths.add(delFilePaths.get(offset));
        offset++;
        continue;
      }
      for (int i = offset; i < batch + offset; i++) {
        batchedDelFiles.add(new HStoreFile(fs, delFilePaths.get(i), conf, compactionCacheConfig,
          BloomType.NONE, true));
      }
      // compact the del files in a batch.
      paths.add(compactDelFilesInBatch(request, batchedDelFiles));
      // move to the next batch.
      offset += batch;
    }
    return compactDelFiles(request, paths);
  }

  /**
   * Compacts the del file in a batch.
   * @param request The compaction request.
   * @param delFiles The del files.
   * @return The path of new del file after merging.
   * @throws IOException if IO failure is encountered
   */
  private Path compactDelFilesInBatch(PartitionedMobCompactionRequest request,
    List<HStoreFile> delFiles) throws IOException {
    // create a scanner for the del files.
    StoreScanner scanner = createScanner(delFiles, ScanType.COMPACT_RETAIN_DELETES);
    StoreFileWriter writer = null;
    Path filePath = null;
    try {
      writer = MobUtils.createDelFileWriter(conf, fs, column,
        MobUtils.formatDate(new Date(request.selectionTime)), tempPath, Long.MAX_VALUE,
        column.getCompactionCompressionType(), HConstants.EMPTY_START_ROW, compactionCacheConfig,
          cryptoContext);
      filePath = writer.getPath();
      List<Cell> cells = new ArrayList<>();
      boolean hasMore;
      ScannerContext scannerContext =
              ScannerContext.newBuilder().setBatchLimit(compactionKVMax).build();
      do {
        hasMore = scanner.next(cells, scannerContext);
        for (Cell cell : cells) {
          writer.append(cell);
        }
        cells.clear();
      } while (hasMore);
    } finally {
      scanner.close();
      if (writer != null) {
        try {
          writer.close();
        } catch (IOException e) {
          LOG.error("Failed to close the writer of the file " + filePath, e);
        }
      }
    }
    // commit the new del file
    Path path = MobUtils.commitFile(conf, fs, filePath, mobFamilyDir, compactionCacheConfig);
    // archive the old del files
    try {
      MobUtils.removeMobFiles(conf, fs, tableName, mobTableDir, column.getName(), delFiles);
    } catch (IOException e) {
      LOG.error("Failed to archive the old del files " + delFiles, e);
    }
    return path;
  }

  /**
   * Creates a store scanner.
   * @param filesToCompact The files to be compacted.
   * @param scanType The scan type.
   * @return The store scanner.
   * @throws IOException if IO failure is encountered
   */
  private StoreScanner createScanner(List<HStoreFile> filesToCompact, ScanType scanType)
      throws IOException {
    List<StoreFileScanner> scanners = StoreFileScanner.getScannersForStoreFiles(filesToCompact,
      false, true, false, false, HConstants.LATEST_TIMESTAMP);
    long ttl = HStore.determineTTLFromFamily(column);
    ScanInfo scanInfo = new ScanInfo(conf, column, ttl, 0, CellComparator.getInstance());
    return new StoreScanner(scanInfo, scanType, scanners);
  }

  /**
   * Bulkloads the current file.
   * @param tableName The table to load into.
   * @param bulkloadDirectory The path of bulkload directory.
   * @param fileName The current file name.
   * @throws IOException if IO failure is encountered
   */
  private void bulkloadRefFile(TableName tableName, Path bulkloadDirectory, String fileName)
      throws IOException {
    // bulkload the ref file
    try {
      BulkLoadHFiles.create(conf).bulkLoad(tableName, bulkloadDirectory);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * Closes the mob file writer.
   * @param writer The mob file writer.
   * @param maxSeqId Maximum sequence id.
   * @param mobCellsCount The number of mob cells.
   * @throws IOException if IO failure is encountered
   */
  private void closeMobFileWriter(StoreFileWriter writer, long maxSeqId, long mobCellsCount)
    throws IOException {
    if (writer != null) {
      writer.appendMetadata(maxSeqId, false, mobCellsCount);
      try {
        writer.close();
      } catch (IOException e) {
        LOG.error("Failed to close the writer of the file " + writer.getPath(), e);
      }
    }
  }

  /**
   * Closes the ref file writer.
   * @param writer The ref file writer.
   * @param maxSeqId Maximum sequence id.
   * @param bulkloadTime The timestamp at which the bulk load file is created.
   * @throws IOException if IO failure is encountered
   */
  private void closeRefFileWriter(StoreFileWriter writer, long maxSeqId, long bulkloadTime)
    throws IOException {
    if (writer != null) {
      writer.appendMetadata(maxSeqId, false);
      writer.appendFileInfo(BULKLOAD_TIME_KEY, Bytes.toBytes(bulkloadTime));
      writer.appendFileInfo(SKIP_RESET_SEQ_ID, Bytes.toBytes(true));
      try {
        writer.close();
      } catch (IOException e) {
        LOG.error("Failed to close the writer of the ref file " + writer.getPath(), e);
      }
    }
  }

  /**
   * Gets the max seqId and number of cells of the store files.
   * @param storeFiles The store files.
   * @return The pair of the max seqId and number of cells of the store files.
   * @throws IOException if IO failure is encountered
   */
  private Pair<Long, Long> getFileInfo(List<HStoreFile> storeFiles) throws IOException {
    long maxSeqId = 0;
    long maxKeyCount = 0;
    for (HStoreFile sf : storeFiles) {
      // the readers will be closed later after the merge.
      maxSeqId = Math.max(maxSeqId, sf.getMaxSequenceId());
      sf.initReader();
      byte[] count = sf.getReader().loadFileInfo().get(MOB_CELLS_COUNT);
      if (count != null) {
        maxKeyCount += Bytes.toLong(count);
      }
    }
    return new Pair<>(maxSeqId, maxKeyCount);
  }

  /**
   * Deletes a file.
   * @param path The path of the file to be deleted.
   */
  private void deletePath(Path path) {
    try {
      if (path != null) {
        fs.delete(path, true);
      }
    } catch (IOException e) {
      LOG.error("Failed to delete the file " + path, e);
    }
  }

  private FileStatus getLinkedFileStatus(HFileLink link) throws IOException {
    Path[] locations = link.getLocations();
    FileStatus file;
    for (Path location : locations) {

      if (location != null) {
        try {
          file = fs.getFileStatus(location);
          if (file != null) {
            return file;
          }
        }  catch (FileNotFoundException e) {
        }
      }
    }
    LOG.warn("The file " + link + " links to can not be found");
    return null;
  }
}
