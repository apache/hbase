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

package org.apache.hadoop.hbase.regionserver.wal;

import java.io.IOException;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.CompactionDescriptor;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.FlushDescriptor;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.RegionEventDescriptor;
import org.apache.hadoop.hbase.util.FSUtils;

import com.google.protobuf.TextFormat;

@InterfaceAudience.Private
public class HLogUtil {
  static final Log LOG = LogFactory.getLog(HLogUtil.class);

  /**
   * Pattern used to validate a HLog file name
   */
  private static final Pattern pattern =
      Pattern.compile(".*\\.\\d*("+HLog.META_HLOG_FILE_EXTN+")*");

  /**
   * @param filename
   *          name of the file to validate
   * @return <tt>true</tt> if the filename matches an HLog, <tt>false</tt>
   *         otherwise
   */
  public static boolean validateHLogFilename(String filename) {
    return pattern.matcher(filename).matches();
  }

  /**
   * Construct the HLog directory name
   *
   * @param serverName
   *          Server name formatted as described in {@link ServerName}
   * @return the relative HLog directory name, e.g.
   *         <code>.logs/1.example.org,60030,12345</code> if
   *         <code>serverName</code> passed is
   *         <code>1.example.org,60030,12345</code>
   */
  public static String getHLogDirectoryName(final String serverName) {
    StringBuilder dirName = new StringBuilder(HConstants.HREGION_LOGDIR_NAME);
    dirName.append("/");
    dirName.append(serverName);
    return dirName.toString();
  }

  /**
   * @param regiondir
   *          This regions directory in the filesystem.
   * @return The directory that holds recovered edits files for the region
   *         <code>regiondir</code>
   */
  public static Path getRegionDirRecoveredEditsDir(final Path regiondir) {
    return new Path(regiondir, HConstants.RECOVERED_EDITS_DIR);
  }
  
  /**
   * Move aside a bad edits file.
   *
   * @param fs
   * @param edits
   *          Edits file to move aside.
   * @return The name of the moved aside file.
   * @throws IOException
   */
  public static Path moveAsideBadEditsFile(final FileSystem fs, final Path edits)
      throws IOException {
    Path moveAsideName = new Path(edits.getParent(), edits.getName() + "."
        + System.currentTimeMillis());
    if (!fs.rename(edits, moveAsideName)) {
      LOG.warn("Rename failed from " + edits + " to " + moveAsideName);
    }
    return moveAsideName;
  }

  /**
   * @param path
   *          - the path to analyze. Expected format, if it's in hlog directory:
   *          / [base directory for hbase] / hbase / .logs / ServerName /
   *          logfile
   * @return null if it's not a log file. Returns the ServerName of the region
   *         server that created this log file otherwise.
   */
  public static ServerName getServerNameFromHLogDirectoryName(
      Configuration conf, String path) throws IOException {
    if (path == null
        || path.length() <= HConstants.HREGION_LOGDIR_NAME.length()) {
      return null;
    }

    if (conf == null) {
      throw new IllegalArgumentException("parameter conf must be set");
    }

    final String rootDir = conf.get(HConstants.HBASE_DIR);
    if (rootDir == null || rootDir.isEmpty()) {
      throw new IllegalArgumentException(HConstants.HBASE_DIR
          + " key not found in conf.");
    }

    final StringBuilder startPathSB = new StringBuilder(rootDir);
    if (!rootDir.endsWith("/"))
      startPathSB.append('/');
    startPathSB.append(HConstants.HREGION_LOGDIR_NAME);
    if (!HConstants.HREGION_LOGDIR_NAME.endsWith("/"))
      startPathSB.append('/');
    final String startPath = startPathSB.toString();

    String fullPath;
    try {
      fullPath = FileSystem.get(conf).makeQualified(new Path(path)).toString();
    } catch (IllegalArgumentException e) {
      LOG.info("Call to makeQualified failed on " + path + " " + e.getMessage());
      return null;
    }

    if (!fullPath.startsWith(startPath)) {
      return null;
    }

    final String serverNameAndFile = fullPath.substring(startPath.length());

    if (serverNameAndFile.indexOf('/') < "a,0,0".length()) {
      // Either it's a file (not a directory) or it's not a ServerName format
      return null;
    }

    Path p = new Path(path);
    return getServerNameFromHLogDirectoryName(p);
  }

  /**
   * This function returns region server name from a log file name which is in either format:
   * hdfs://<name node>/hbase/.logs/<server name>-splitting/... or hdfs://<name
   * node>/hbase/.logs/<server name>/...
   * @param logFile
   * @return null if the passed in logFile isn't a valid HLog file path
   */
  public static ServerName getServerNameFromHLogDirectoryName(Path logFile) {
    Path logDir = logFile.getParent();
    String logDirName = logDir.getName();
    if (logDirName.equals(HConstants.HREGION_LOGDIR_NAME)) {
      logDir = logFile;
      logDirName = logDir.getName();
    }
    ServerName serverName = null;
    if (logDirName.endsWith(HLog.SPLITTING_EXT)) {
      logDirName = logDirName.substring(0, logDirName.length() - HLog.SPLITTING_EXT.length());
    }
    try {
      serverName = ServerName.parseServerName(logDirName);
    } catch (IllegalArgumentException ex) {
      serverName = null;
      LOG.warn("Cannot parse a server name from path=" + logFile + "; " + ex.getMessage());
    }
    if (serverName != null && serverName.getStartcode() < 0) {
      LOG.warn("Invalid log file path=" + logFile);
      return null;
    }
    return serverName;
  }

  /**
   * Returns sorted set of edit files made by wal-log splitter, excluding files
   * with '.temp' suffix.
   *
   * @param fs
   * @param regiondir
   * @return Files in passed <code>regiondir</code> as a sorted set.
   * @throws IOException
   */
  public static NavigableSet<Path> getSplitEditFilesSorted(final FileSystem fs,
      final Path regiondir) throws IOException {
    NavigableSet<Path> filesSorted = new TreeSet<Path>();
    Path editsdir = HLogUtil.getRegionDirRecoveredEditsDir(regiondir);
    if (!fs.exists(editsdir))
      return filesSorted;
    FileStatus[] files = FSUtils.listStatus(fs, editsdir, new PathFilter() {
      @Override
      public boolean accept(Path p) {
        boolean result = false;
        try {
          // Return files and only files that match the editfile names pattern.
          // There can be other files in this directory other than edit files.
          // In particular, on error, we'll move aside the bad edit file giving
          // it a timestamp suffix. See moveAsideBadEditsFile.
          Matcher m = HLog.EDITFILES_NAME_PATTERN.matcher(p.getName());
          result = fs.isFile(p) && m.matches();
          // Skip the file whose name ends with RECOVERED_LOG_TMPFILE_SUFFIX,
          // because it means splithlog thread is writting this file.
          if (p.getName().endsWith(HLog.RECOVERED_LOG_TMPFILE_SUFFIX)) {
            result = false;
          }
        } catch (IOException e) {
          LOG.warn("Failed isFile check on " + p);
        }
        return result;
      }
    });
    if (files == null)
      return filesSorted;
    for (FileStatus status : files) {
      filesSorted.add(status.getPath());
    }
    return filesSorted;
  }

  public static boolean isMetaFile(Path p) {
    return isMetaFile(p.getName());
  }

  public static boolean isMetaFile(String p) {
    if (p != null && p.endsWith(HLog.META_HLOG_FILE_EXTN)) {
      return true;
    }
    return false;
  }

  /**
   * Write the marker that a compaction has succeeded and is about to be committed.
   * This provides info to the HMaster to allow it to recover the compaction if
   * this regionserver dies in the middle (This part is not yet implemented). It also prevents
   * the compaction from finishing if this regionserver has already lost its lease on the log.
   * @param sequenceId Used by HLog to get sequence Id for the waledit.
   */
  public static void writeCompactionMarker(HLog log, HTableDescriptor htd, HRegionInfo info,
      final CompactionDescriptor c, AtomicLong sequenceId) throws IOException {
    TableName tn = TableName.valueOf(c.getTableName().toByteArray());
    HLogKey key = new HLogKey(info.getEncodedNameAsBytes(), tn);
    log.appendNoSync(htd, info, key, WALEdit.createCompaction(info, c), sequenceId, false, null);
    log.sync();
    if (LOG.isTraceEnabled()) {
      LOG.trace("Appended compaction marker " + TextFormat.shortDebugString(c));
    }
  }

  /**
   * Write a flush marker indicating a start / abort or a complete of a region flush
   */
  public static long writeFlushMarker(HLog log, HTableDescriptor htd, HRegionInfo info,
      final FlushDescriptor f, AtomicLong sequenceId, boolean sync) throws IOException {
    TableName tn = TableName.valueOf(f.getTableName().toByteArray());
    HLogKey key = new HLogKey(info.getEncodedNameAsBytes(), tn);
    long trx = log.appendNoSync(htd, info, key, WALEdit.createFlushWALEdit(info, f), sequenceId, false, null);
    if (sync) log.sync(trx);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Appended flush marker " + TextFormat.shortDebugString(f));
    }
    return trx;
  }

  /**
   * Write a region open marker indicating that the region is opened
   */
  public static long writeRegionEventMarker(HLog log, HTableDescriptor htd, HRegionInfo info,
      final RegionEventDescriptor r, AtomicLong sequenceId) throws IOException {
    TableName tn = TableName.valueOf(r.getTableName().toByteArray());
    HLogKey key = new HLogKey(info.getEncodedNameAsBytes(), tn);
    long trx = log.appendNoSync(htd, info, key, WALEdit.createRegionEventWALEdit(info, r),
      sequenceId, false, null);
    log.sync(trx);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Appended region event marker " + TextFormat.shortDebugString(r));
    }
    return trx;
  }
  
  /**
   * Create a file with name as region open sequence id
   * 
   * @param fs
   * @param regiondir
   * @param newSeqId
   * @param saftyBumper
   * @return long new sequence Id value
   * @throws IOException
   */
  public static long writeRegionOpenSequenceIdFile(final FileSystem fs, final Path regiondir,
      long newSeqId, long saftyBumper) throws IOException {

    Path editsdir = HLogUtil.getRegionDirRecoveredEditsDir(regiondir);
    long maxSeqId = 0;
    FileStatus[] files = null;
    if (fs.exists(editsdir)) {
      files = FSUtils.listStatus(fs, editsdir, new PathFilter() {
        @Override
        public boolean accept(Path p) {
          if (p.getName().endsWith(HLog.SEQUENCE_ID_FILE_SUFFIX)) {
            return true;
          }
          return false;
        }
      });
      if (files != null) {
        for (FileStatus status : files) {
          String fileName = status.getPath().getName();
          try {
            Long tmpSeqId = Long.parseLong(fileName.substring(0, fileName.length()
                    - HLog.SEQUENCE_ID_FILE_SUFFIX.length()));
            maxSeqId = Math.max(tmpSeqId, maxSeqId);
          } catch (NumberFormatException ex) {
            LOG.warn("Invalid SeqId File Name=" + fileName);
          }
        }
      }
    }
    if (maxSeqId > newSeqId) {
      newSeqId = maxSeqId;
    }
    newSeqId += saftyBumper; // bump up SeqId
    
    // write a new seqId file
    Path newSeqIdFile = new Path(editsdir, newSeqId + HLog.SEQUENCE_ID_FILE_SUFFIX);
    if (!fs.createNewFile(newSeqIdFile)) {
      throw new IOException("Failed to create SeqId file:" + newSeqIdFile);
    }
    // remove old ones
    if(files != null) {
      for (FileStatus status : files) {
        fs.delete(status.getPath(), false);
      }
    }
    return newSeqId;
  }
  
}
