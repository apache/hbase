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
package org.apache.hadoop.hbase.wal;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.AbstractFSWAL;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ConcurrentMapUtils.IOExceptionSupplier;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.zookeeper.ZKSplitLog;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;

/**
 * This class provides static methods to support WAL splitting related works
 */
@InterfaceAudience.Private
public final class WALSplitUtil {
  private static final Logger LOG = LoggerFactory.getLogger(WALSplitUtil.class);

  private static final Pattern EDITFILES_NAME_PATTERN = Pattern.compile("-?[0-9]+");
  private static final String RECOVERED_LOG_TMPFILE_SUFFIX = ".temp";
  private static final String SEQUENCE_ID_FILE_SUFFIX = ".seqid";
  private static final String OLD_SEQUENCE_ID_FILE_SUFFIX = "_seqid";
  private static final int SEQUENCE_ID_FILE_SUFFIX_LENGTH = SEQUENCE_ID_FILE_SUFFIX.length();

  private WALSplitUtil() {
  }

  /**
   * Completes the work done by splitLogFile by archiving logs
   * <p>
   * It is invoked by SplitLogManager once it knows that one of the SplitLogWorkers have completed
   * the splitLogFile() part. If the master crashes then this function might get called multiple
   * times.
   * <p>
   * @param logfile
   * @param conf
   * @throws IOException
   */
  public static void finishSplitLogFile(String logfile, Configuration conf) throws IOException {
    Path walDir = FSUtils.getWALRootDir(conf);
    Path oldLogDir = new Path(walDir, HConstants.HREGION_OLDLOGDIR_NAME);
    Path walPath;
    if (FSUtils.isStartingWithPath(walDir, logfile)) {
      walPath = new Path(logfile);
    } else {
      walPath = new Path(walDir, logfile);
    }
    finishSplitLogFile(walDir, oldLogDir, walPath, conf);
  }

  static void finishSplitLogFile(Path walDir, Path oldWALDir, Path walPath,
      Configuration conf) throws IOException {
    List<Path> processedLogs = new ArrayList<>();
    List<Path> corruptedLogs = new ArrayList<>();
    FileSystem walFS = walDir.getFileSystem(conf);
    if (ZKSplitLog.isCorrupted(walDir, walPath.getName(), walFS)) {
      corruptedLogs.add(walPath);
    } else {
      processedLogs.add(walPath);
    }
    archiveWALs(corruptedLogs, processedLogs, oldWALDir, walFS, conf);
    Path stagingDir = ZKSplitLog.getSplitLogDir(walDir, walPath.getName());
    walFS.delete(stagingDir, true);
  }

  /**
   * Moves processed logs to a oldLogDir after successful processing Moves corrupted logs (any log
   * that couldn't be successfully parsed to corruptDir (.corrupt) for later investigation
   */
  private static void archiveWALs(final List<Path> corruptedWALs, final List<Path> processedWALs,
      final Path oldWALDir, final FileSystem walFS, final Configuration conf) throws IOException {
    final Path corruptDir = new Path(FSUtils.getWALRootDir(conf), HConstants.CORRUPT_DIR_NAME);
    if (conf.get("hbase.regionserver.hlog.splitlog.corrupt.dir") != null) {
      LOG.warn("hbase.regionserver.hlog.splitlog.corrupt.dir is deprecated. Default to {}",
        corruptDir);
    }
    if (!walFS.mkdirs(corruptDir)) {
      LOG.info("Unable to mkdir {}", corruptDir);
    }
    walFS.mkdirs(oldWALDir);

    // this method can get restarted or called multiple times for archiving
    // the same log files.
    for (Path corruptedWAL : corruptedWALs) {
      Path p = new Path(corruptDir, corruptedWAL.getName());
      if (walFS.exists(corruptedWAL)) {
        if (!walFS.rename(corruptedWAL, p)) {
          LOG.warn("Unable to move corrupted log {} to {}", corruptedWAL, p);
        } else {
          LOG.warn("Moved corrupted log {} to {}", corruptedWAL, p);
        }
      }
    }

    for (Path p : processedWALs) {
      Path newPath = AbstractFSWAL.getWALArchivePath(oldWALDir, p);
      if (walFS.exists(p)) {
        if (!FSUtils.renameAndSetModifyTime(walFS, p, newPath)) {
          LOG.warn("Unable to move {} to {}", p, newPath);
        } else {
          LOG.info("Archived processed log {} to {}", p, newPath);
        }
      }
    }
  }

  /**
   * Path to a file under RECOVERED_EDITS_DIR directory of the region found in <code>logEntry</code>
   * named for the sequenceid in the passed <code>logEntry</code>: e.g.
   * /hbase/some_table/2323432434/recovered.edits/2332. This method also ensures existence of
   * RECOVERED_EDITS_DIR under the region creating it if necessary.
   * @param tableName the table name
   * @param encodedRegionName the encoded region name
   * @param sedId the sequence id which used to generate file name
   * @param fileNameBeingSplit the file being split currently. Used to generate tmp file name.
   * @param tmpDirName of the directory used to sideline old recovered edits file
   * @param conf configuration
   * @return Path to file into which to dump split log edits.
   * @throws IOException
   */
  @SuppressWarnings("deprecation")
  @VisibleForTesting
  static Path getRegionSplitEditsPath(TableName tableName, byte[] encodedRegionName, long sedId,
      String fileNameBeingSplit, String tmpDirName, Configuration conf) throws IOException {
    FileSystem walFS = FSUtils.getWALFileSystem(conf);
    Path tableDir = FSUtils.getWALTableDir(conf, tableName);
    String encodedRegionNameStr = Bytes.toString(encodedRegionName);
    Path regionDir = HRegion.getRegionDir(tableDir, encodedRegionNameStr);
    Path dir = getRegionDirRecoveredEditsDir(regionDir);

    if (walFS.exists(dir) && walFS.isFile(dir)) {
      Path tmp = new Path(tmpDirName);
      if (!walFS.exists(tmp)) {
        walFS.mkdirs(tmp);
      }
      tmp = new Path(tmp, HConstants.RECOVERED_EDITS_DIR + "_" + encodedRegionNameStr);
      LOG.warn("Found existing old file: {}. It could be some "
          + "leftover of an old installation. It should be a folder instead. "
          + "So moving it to {}",
        dir, tmp);
      if (!walFS.rename(dir, tmp)) {
        LOG.warn("Failed to sideline old file {}", dir);
      }
    }

    if (!walFS.exists(dir) && !walFS.mkdirs(dir)) {
      LOG.warn("mkdir failed on {}", dir);
    }
    // Append fileBeingSplit to prevent name conflict since we may have duplicate wal entries now.
    // Append file name ends with RECOVERED_LOG_TMPFILE_SUFFIX to ensure
    // region's replayRecoveredEdits will not delete it
    String fileName = formatRecoveredEditsFileName(sedId);
    fileName = getTmpRecoveredEditsFileName(fileName + "-" + fileNameBeingSplit);
    return new Path(dir, fileName);
  }

  private static String getTmpRecoveredEditsFileName(String fileName) {
    return fileName + RECOVERED_LOG_TMPFILE_SUFFIX;
  }

  /**
   * Get the completed recovered edits file path, renaming it to be by last edit in the file from
   * its first edit. Then we could use the name to skip recovered edits when doing
   * {@link HRegion#replayRecoveredEditsIfAny}.
   * @return dstPath take file's last edit log seq num as the name
   */
  static Path getCompletedRecoveredEditsFilePath(Path srcPath, long maximumEditWALSeqNum) {
    String fileName = formatRecoveredEditsFileName(maximumEditWALSeqNum);
    return new Path(srcPath.getParent(), fileName);
  }

  @VisibleForTesting
  static String formatRecoveredEditsFileName(final long seqid) {
    return String.format("%019d", seqid);
  }

  /**
   * @param regionDir This regions directory in the filesystem.
   * @return The directory that holds recovered edits files for the region <code>regionDir</code>
   */
  public static Path getRegionDirRecoveredEditsDir(final Path regionDir) {
    return new Path(regionDir, HConstants.RECOVERED_EDITS_DIR);
  }

  /**
   * Check whether there is recovered.edits in the region dir
   * @param conf conf
   * @param regionInfo the region to check
   * @return true if recovered.edits exist in the region dir
   */
  public static boolean hasRecoveredEdits(final Configuration conf, final RegionInfo regionInfo)
      throws IOException {
    // No recovered.edits for non default replica regions
    if (regionInfo.getReplicaId() != RegionInfo.DEFAULT_REPLICA_ID) {
      return false;
    }
    // Only default replica region can reach here, so we can use regioninfo
    // directly without converting it to default replica's regioninfo.
    Path regionWALDir =
        FSUtils.getWALRegionDir(conf, regionInfo.getTable(), regionInfo.getEncodedName());
    Path regionDir = FSUtils.getRegionDirFromRootDir(FSUtils.getRootDir(conf), regionInfo);
    Path wrongRegionWALDir =
        FSUtils.getWrongWALRegionDir(conf, regionInfo.getTable(), regionInfo.getEncodedName());
    FileSystem walFs = FSUtils.getWALFileSystem(conf);
    FileSystem rootFs = FSUtils.getRootDirFileSystem(conf);
    NavigableSet<Path> files = getSplitEditFilesSorted(walFs, regionWALDir);
    if (!files.isEmpty()) {
      return true;
    }
    files = getSplitEditFilesSorted(rootFs, regionDir);
    if (!files.isEmpty()) {
      return true;
    }
    files = getSplitEditFilesSorted(walFs, wrongRegionWALDir);
    return !files.isEmpty();
  }

  /**
   * This method will check 3 places for finding the max sequence id file. One is the expected
   * place, another is the old place under the region directory, and the last one is the wrong one
   * we introduced in HBASE-20734. See HBASE-22617 for more details.
   * <p/>
   * Notice that, you should always call this method instead of
   * {@link #getMaxRegionSequenceId(FileSystem, Path)} until 4.0.0 release.
   * @deprecated Only for compatibility, will be removed in 4.0.0.
   */
  @Deprecated
  public static long getMaxRegionSequenceId(Configuration conf, RegionInfo region,
      IOExceptionSupplier<FileSystem> rootFsSupplier, IOExceptionSupplier<FileSystem> walFsSupplier)
      throws IOException {
    FileSystem rootFs = rootFsSupplier.get();
    FileSystem walFs = walFsSupplier.get();
    Path regionWALDir = FSUtils.getWALRegionDir(conf, region.getTable(), region.getEncodedName());
    // This is the old place where we store max sequence id file
    Path regionDir = FSUtils.getRegionDirFromRootDir(FSUtils.getRootDir(conf), region);
    // This is for HBASE-20734, where we use a wrong directory, see HBASE-22617 for more details.
    Path wrongRegionWALDir =
      FSUtils.getWrongWALRegionDir(conf, region.getTable(), region.getEncodedName());
    long maxSeqId = getMaxRegionSequenceId(walFs, regionWALDir);
    maxSeqId = Math.max(maxSeqId, getMaxRegionSequenceId(rootFs, regionDir));
    maxSeqId = Math.max(maxSeqId, getMaxRegionSequenceId(walFs, wrongRegionWALDir));
    return maxSeqId;
  }

  /**
   * Returns sorted set of edit files made by splitter, excluding files with '.temp' suffix.
   * @param walFS WAL FileSystem used to retrieving split edits files.
   * @param regionDir WAL region dir to look for recovered edits files under.
   * @return Files in passed <code>regionDir</code> as a sorted set.
   * @throws IOException
   */
  public static NavigableSet<Path> getSplitEditFilesSorted(final FileSystem walFS,
      final Path regionDir) throws IOException {
    NavigableSet<Path> filesSorted = new TreeSet<>();
    Path editsdir = getRegionDirRecoveredEditsDir(regionDir);
    if (!walFS.exists(editsdir)) {
      return filesSorted;
    }
    FileStatus[] files = FSUtils.listStatus(walFS, editsdir, new PathFilter() {
      @Override
      public boolean accept(Path p) {
        boolean result = false;
        try {
          // Return files and only files that match the editfile names pattern.
          // There can be other files in this directory other than edit files.
          // In particular, on error, we'll move aside the bad edit file giving
          // it a timestamp suffix. See moveAsideBadEditsFile.
          Matcher m = EDITFILES_NAME_PATTERN.matcher(p.getName());
          result = walFS.isFile(p) && m.matches();
          // Skip the file whose name ends with RECOVERED_LOG_TMPFILE_SUFFIX,
          // because it means splitwal thread is writting this file.
          if (p.getName().endsWith(RECOVERED_LOG_TMPFILE_SUFFIX)) {
            result = false;
          }
          // Skip SeqId Files
          if (isSequenceIdFile(p)) {
            result = false;
          }
        } catch (IOException e) {
          LOG.warn("Failed isFile check on {}", p, e);
        }
        return result;
      }
    });
    if (ArrayUtils.isNotEmpty(files)) {
      Arrays.asList(files).forEach(status -> filesSorted.add(status.getPath()));
    }
    return filesSorted;
  }

  /**
   * Move aside a bad edits file.
   * @param walFS WAL FileSystem used to rename bad edits file.
   * @param edits Edits file to move aside.
   * @return The name of the moved aside file.
   * @throws IOException
   */
  public static Path moveAsideBadEditsFile(final FileSystem walFS, final Path edits)
      throws IOException {
    Path moveAsideName =
        new Path(edits.getParent(), edits.getName() + "." + System.currentTimeMillis());
    if (!walFS.rename(edits, moveAsideName)) {
      LOG.warn("Rename failed from {} to {}", edits, moveAsideName);
    }
    return moveAsideName;
  }

  /**
   * Is the given file a region open sequence id file.
   */
  @VisibleForTesting
  public static boolean isSequenceIdFile(final Path file) {
    return file.getName().endsWith(SEQUENCE_ID_FILE_SUFFIX)
        || file.getName().endsWith(OLD_SEQUENCE_ID_FILE_SUFFIX);
  }

  private static FileStatus[] getSequenceIdFiles(FileSystem walFS, Path regionDir)
      throws IOException {
    // TODO: Why are we using a method in here as part of our normal region open where
    // there is no splitting involved? Fix. St.Ack 01/20/2017.
    Path editsDir = getRegionDirRecoveredEditsDir(regionDir);
    try {
      FileStatus[] files = walFS.listStatus(editsDir, WALSplitUtil::isSequenceIdFile);
      return files != null ? files : new FileStatus[0];
    } catch (FileNotFoundException e) {
      return new FileStatus[0];
    }
  }

  private static long getMaxSequenceId(FileStatus[] files) {
    long maxSeqId = -1L;
    for (FileStatus file : files) {
      String fileName = file.getPath().getName();
      try {
        maxSeqId = Math.max(maxSeqId, Long
            .parseLong(fileName.substring(0, fileName.length() - SEQUENCE_ID_FILE_SUFFIX_LENGTH)));
      } catch (NumberFormatException ex) {
        LOG.warn("Invalid SeqId File Name={}", fileName);
      }
    }
    return maxSeqId;
  }

  /**
   * Get the max sequence id which is stored in the region directory. -1 if none.
   */
  public static long getMaxRegionSequenceId(FileSystem walFS, Path regionDir) throws IOException {
    return getMaxSequenceId(getSequenceIdFiles(walFS, regionDir));
  }

  /**
   * Create a file with name as region's max sequence id
   */
  public static void writeRegionSequenceIdFile(FileSystem walFS, Path regionDir, long newMaxSeqId)
      throws IOException {
    FileStatus[] files = getSequenceIdFiles(walFS, regionDir);
    long maxSeqId = getMaxSequenceId(files);
    if (maxSeqId > newMaxSeqId) {
      throw new IOException("The new max sequence id " + newMaxSeqId
          + " is less than the old max sequence id " + maxSeqId);
    }
    // write a new seqId file
    Path newSeqIdFile =
        new Path(getRegionDirRecoveredEditsDir(regionDir), newMaxSeqId + SEQUENCE_ID_FILE_SUFFIX);
    if (newMaxSeqId != maxSeqId) {
      try {
        if (!walFS.createNewFile(newSeqIdFile) && !walFS.exists(newSeqIdFile)) {
          throw new IOException("Failed to create SeqId file:" + newSeqIdFile);
        }
        LOG.debug("Wrote file={}, newMaxSeqId={}, maxSeqId={}", newSeqIdFile, newMaxSeqId,
          maxSeqId);
      } catch (FileAlreadyExistsException ignored) {
        // latest hdfs throws this exception. it's all right if newSeqIdFile already exists
      }
    }
    // remove old ones
    for (FileStatus status : files) {
      if (!newSeqIdFile.equals(status.getPath())) {
        walFS.delete(status.getPath(), false);
      }
    }
  }

  /** A struct used by getMutationsFromWALEntry */
  public static class MutationReplay implements Comparable<MutationReplay> {
    public MutationReplay(ClientProtos.MutationProto.MutationType type, Mutation mutation,
        long nonceGroup, long nonce) {
      this.type = type;
      this.mutation = mutation;
      if (this.mutation.getDurability() != Durability.SKIP_WAL) {
        // using ASYNC_WAL for relay
        this.mutation.setDurability(Durability.ASYNC_WAL);
      }
      this.nonceGroup = nonceGroup;
      this.nonce = nonce;
    }

    private final ClientProtos.MutationProto.MutationType type;
    public final Mutation mutation;
    public final long nonceGroup;
    public final long nonce;

    @Override
    public int compareTo(final MutationReplay d) {
      return this.mutation.compareTo(d.mutation);
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof MutationReplay)) {
        return false;
      } else {
        return this.compareTo((MutationReplay) obj) == 0;
      }
    }

    @Override
    public int hashCode() {
      return this.mutation.hashCode();
    }

    public ClientProtos.MutationProto.MutationType getType() {
      return type;
    }
  }

  /**
   * This function is used to construct mutations from a WALEntry. It also reconstructs WALKey &amp;
   * WALEdit from the passed in WALEntry
   * @param entry
   * @param cells
   * @param logEntry pair of WALKey and WALEdit instance stores WALKey and WALEdit instances
   *          extracted from the passed in WALEntry.
   * @return list of Pair&lt;MutationType, Mutation&gt; to be replayed
   * @throws IOException
   */
  public static List<MutationReplay> getMutationsFromWALEntry(AdminProtos.WALEntry entry,
      CellScanner cells, Pair<WALKey, WALEdit> logEntry, Durability durability) throws IOException {
    if (entry == null) {
      // return an empty array
      return Collections.emptyList();
    }

    long replaySeqId =
        (entry.getKey().hasOrigSequenceNumber()) ? entry.getKey().getOrigSequenceNumber()
            : entry.getKey().getLogSequenceNumber();
    int count = entry.getAssociatedCellCount();
    List<MutationReplay> mutations = new ArrayList<>();
    Cell previousCell = null;
    Mutation m = null;
    WALKeyImpl key = null;
    WALEdit val = null;
    if (logEntry != null) {
      val = new WALEdit();
    }

    for (int i = 0; i < count; i++) {
      // Throw index out of bounds if our cell count is off
      if (!cells.advance()) {
        throw new ArrayIndexOutOfBoundsException("Expected=" + count + ", index=" + i);
      }
      Cell cell = cells.current();
      if (val != null) val.add(cell);

      boolean isNewRowOrType =
          previousCell == null || previousCell.getTypeByte() != cell.getTypeByte()
              || !CellUtil.matchingRows(previousCell, cell);
      if (isNewRowOrType) {
        // Create new mutation
        if (CellUtil.isDelete(cell)) {
          m = new Delete(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
          // Deletes don't have nonces.
          mutations.add(new MutationReplay(ClientProtos.MutationProto.MutationType.DELETE, m,
              HConstants.NO_NONCE, HConstants.NO_NONCE));
        } else {
          m = new Put(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
          // Puts might come from increment or append, thus we need nonces.
          long nonceGroup =
              entry.getKey().hasNonceGroup() ? entry.getKey().getNonceGroup() : HConstants.NO_NONCE;
          long nonce = entry.getKey().hasNonce() ? entry.getKey().getNonce() : HConstants.NO_NONCE;
          mutations.add(
            new MutationReplay(ClientProtos.MutationProto.MutationType.PUT, m, nonceGroup, nonce));
        }
      }
      if (CellUtil.isDelete(cell)) {
        ((Delete) m).add(cell);
      } else {
        ((Put) m).add(cell);
      }
      m.setDurability(durability);
      previousCell = cell;
    }

    // reconstruct WALKey
    if (logEntry != null) {
      org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.WALKey walKeyProto =
          entry.getKey();
      List<UUID> clusterIds = new ArrayList<>(walKeyProto.getClusterIdsCount());
      for (HBaseProtos.UUID uuid : entry.getKey().getClusterIdsList()) {
        clusterIds.add(new UUID(uuid.getMostSigBits(), uuid.getLeastSigBits()));
      }
      key = new WALKeyImpl(walKeyProto.getEncodedRegionName().toByteArray(),
          TableName.valueOf(walKeyProto.getTableName().toByteArray()), replaySeqId,
          walKeyProto.getWriteTime(), clusterIds, walKeyProto.getNonceGroup(),
          walKeyProto.getNonce(), null);
      logEntry.setFirst(key);
      logEntry.setSecond(val);
    }

    return mutations;
  }
}
