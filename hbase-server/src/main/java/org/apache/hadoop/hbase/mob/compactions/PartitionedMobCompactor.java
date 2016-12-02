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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ArrayBackedTag;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.TagType;
import org.apache.hadoop.hbase.TagUtil;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobFileName;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.mob.compactions.MobCompactionRequest.CompactionType;
import org.apache.hadoop.hbase.mob.compactions.PartitionedMobCompactionRequest.CompactionPartition;
import org.apache.hadoop.hbase.mob.compactions.PartitionedMobCompactionRequest.CompactionPartitionId;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.ScanInfo;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;
import org.apache.hadoop.hbase.regionserver.StoreFileWriter;
import org.apache.hadoop.hbase.regionserver.StoreScanner;
import org.apache.hadoop.hbase.security.EncryptionUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

/**
 * An implementation of {@link MobCompactor} that compacts the mob files in partitions.
 */
@InterfaceAudience.Private
public class PartitionedMobCompactor extends MobCompactor {

  private static final Log LOG = LogFactory.getLog(PartitionedMobCompactor.class);
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
    HColumnDescriptor column, ExecutorService pool) throws IOException {
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
    Collection<FileStatus> allDelFiles = new ArrayList<>();
    Map<CompactionPartitionId, CompactionPartition> filesToCompact = new HashMap<>();
    int selectedFileCount = 0;
    int irrelevantFileCount = 0;
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
      if (StoreFileInfo.isDelFile(linkedFile.getPath())) {
        allDelFiles.add(file);
      } else if (allFiles || linkedFile.getLen() < mergeableSize) {
        // add all files if allFiles is true,
        // otherwise add the small files to the merge pool
        MobFileName fileName = MobFileName.create(linkedFile.getPath().getName());
        CompactionPartitionId id = new CompactionPartitionId(fileName.getStartKey(),
          fileName.getDate());
        CompactionPartition compactionPartition = filesToCompact.get(id);
        if (compactionPartition == null) {
          compactionPartition = new CompactionPartition(id);
          compactionPartition.addFile(file);
          filesToCompact.put(id, compactionPartition);
        } else {
          compactionPartition.addFile(file);
        }
        selectedFileCount++;
      }
    }
    PartitionedMobCompactionRequest request = new PartitionedMobCompactionRequest(
      filesToCompact.values(), allDelFiles);
    if (candidates.size() == (allDelFiles.size() + selectedFileCount + irrelevantFileCount)) {
      // all the files are selected
      request.setCompactionType(CompactionType.ALL_FILES);
    }
    LOG.info("The compaction type is " + request.getCompactionType() + ", the request has "
      + allDelFiles.size() + " del files, " + selectedFileCount + " selected files, and "
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
    // merge the del files
    List<Path> delFilePaths = new ArrayList<>();
    for (FileStatus delFile : request.delFiles) {
      delFilePaths.add(delFile.getPath());
    }
    List<Path> newDelPaths = compactDelFiles(request, delFilePaths);
    List<StoreFile> newDelFiles = new ArrayList<>();
    List<Path> paths = null;
    try {
      for (Path newDelPath : newDelPaths) {
        StoreFile sf = new StoreFile(fs, newDelPath, conf, compactionCacheConfig, BloomType.NONE);
        // pre-create reader of a del file to avoid race condition when opening the reader in each
        // partition.
        sf.createReader();
        newDelFiles.add(sf);
      }
      LOG.info("After merging, there are " + newDelFiles.size() + " del files");
      // compact the mob files by partitions.
      paths = compactMobFiles(request, newDelFiles);
      LOG.info("After compaction, there are " + paths.size() + " mob files");
    } finally {
      closeStoreFileReaders(newDelFiles);
    }
    // archive the del files if all the mob files are selected.
    if (request.type == CompactionType.ALL_FILES && !newDelPaths.isEmpty()) {
      LOG.info(
          "After a mob compaction with all files selected, archiving the del files " + newDelPaths);
      try {
        MobUtils.removeMobFiles(conf, fs, tableName, mobTableDir, column.getName(), newDelFiles);
      } catch (IOException e) {
        LOG.error("Failed to archive the del files " + newDelPaths, e);
      }
    }
    return paths;
  }

  /**
   * Compacts the selected small mob files and all the del files.
   * @param request The compaction request.
   * @param delFiles The del files.
   * @return The paths of new mob files after compactions.
   * @throws IOException if IO failure is encountered
   */
  protected List<Path> compactMobFiles(final PartitionedMobCompactionRequest request,
    final List<StoreFile> delFiles) throws IOException {
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
   * @param connection to use
   * @param table The current table.  @return The paths of new mob files after compactions.
   * @throws IOException if IO failure is encountered
   */
  private List<Path> compactMobFilePartition(PartitionedMobCompactionRequest request,
                                             CompactionPartition partition,
                                             List<StoreFile> delFiles,
                                             Connection connection,
                                             Table table) throws IOException {
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
      List<StoreFile> filesToCompact = new ArrayList<>();
      for (int i = offset; i < batch + offset; i++) {
        StoreFile sf = new StoreFile(fs, files.get(i).getPath(), conf, compactionCacheConfig,
          BloomType.NONE);
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
  private void closeStoreFileReaders(List<StoreFile> storeFiles) {
    for (StoreFile storeFile : storeFiles) {
      try {
        storeFile.closeReader(true);
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
                                      List<StoreFile> filesToCompact, int batch,
                                      Path bulkloadPathOfPartition, Path bulkloadColumnPath,
                                      List<Path> newFiles)
      throws IOException {
    // open scanner to the selected mob files and del files.
    StoreScanner scanner = createScanner(filesToCompact, ScanType.COMPACT_DROP_DELETES);
    // the mob files to be compacted, not include the del files.
    List<StoreFile> mobFilesToCompact = filesToCompact.subList(0, batch);
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
            .createWriter(conf, fs, column, partition.getPartitionId().getDate(), tempPath,
                Long.MAX_VALUE, column.getCompactionCompressionType(),
                partition.getPartitionId().getStartKey(), compactionCacheConfig, cryptoContext);
        cleanupTmpMobFile = true;
        filePath = writer.getPath();
        byte[] fileName = Bytes.toBytes(filePath.getName());
        // create a temp file and open a writer for it in the bulkloadPath
        refFileWriter = MobUtils.createRefFileWriter(conf, fs, column, bulkloadColumnPath,
            fileInfo.getSecond().longValue(), compactionCacheConfig, cryptoContext);
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
        bulkloadRefFile(connection, table, bulkloadPathOfPartition, filePath.getName());
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
      List<StoreFile> batchedDelFiles = new ArrayList<>();
      if (batch == 1) {
        // only one file left, do not compact it, directly add it to the new files.
        paths.add(delFilePaths.get(offset));
        offset++;
        continue;
      }
      for (int i = offset; i < batch + offset; i++) {
        batchedDelFiles.add(new StoreFile(fs, delFilePaths.get(i), conf, compactionCacheConfig,
          BloomType.NONE));
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
    List<StoreFile> delFiles) throws IOException {
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
  private StoreScanner createScanner(List<StoreFile> filesToCompact, ScanType scanType)
    throws IOException {
    List scanners = StoreFileScanner.getScannersForStoreFiles(filesToCompact, false, true, false,
      false, HConstants.LATEST_TIMESTAMP);
    Scan scan = new Scan();
    scan.setMaxVersions(column.getMaxVersions());
    long ttl = HStore.determineTTLFromFamily(column);
    ScanInfo scanInfo = new ScanInfo(conf, column, ttl, 0, CellComparator.COMPARATOR);
    return new StoreScanner(scan, scanInfo, scanType, null, scanners, 0L,
      HConstants.LATEST_TIMESTAMP);
  }

  /**
   * Bulkloads the current file.
   *
   * @param connection to use to get admin/RegionLocator
   * @param table The current table.
   * @param bulkloadDirectory The path of bulkload directory.
   * @param fileName The current file name.
   * @throws IOException if IO failure is encountered
   */
  private void bulkloadRefFile(Connection connection, Table table, Path bulkloadDirectory,
      String fileName)
      throws IOException {
    // bulkload the ref file
    try {
      LoadIncrementalHFiles bulkload = new LoadIncrementalHFiles(conf);
      bulkload.doBulkLoad(bulkloadDirectory, connection.getAdmin(), table,
          connection.getRegionLocator(table.getName()));
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
      writer.appendFileInfo(StoreFile.BULKLOAD_TIME_KEY, Bytes.toBytes(bulkloadTime));
      writer.appendFileInfo(StoreFile.SKIP_RESET_SEQ_ID, Bytes.toBytes(true));
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
  private Pair<Long, Long> getFileInfo(List<StoreFile> storeFiles) throws IOException {
    long maxSeqId = 0;
    long maxKeyCount = 0;
    for (StoreFile sf : storeFiles) {
      // the readers will be closed later after the merge.
      maxSeqId = Math.max(maxSeqId, sf.getMaxSequenceId());
      byte[] count = sf.createReader().loadFileInfo().get(StoreFile.MOB_CELLS_COUNT);
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
    for (Path location : locations) {
      FileStatus file = getFileStatus(location);
      if (file != null) {
        return file;
      }
    }
    return null;
  }

  private FileStatus getFileStatus(Path path) throws IOException {
    try {
      if (path != null) {
        return fs.getFileStatus(path);
      }
    } catch (FileNotFoundException e) {
      LOG.warn("The file " + path + " can not be found", e);
    }
    return null;
  }
}
