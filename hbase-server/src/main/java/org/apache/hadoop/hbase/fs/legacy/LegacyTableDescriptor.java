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
package org.apache.hadoop.hbase.fs.legacy;

import java.io.IOException;
import java.util.Comparator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableInfoMissingException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.*;

/**
 * Implementation of {@link TableDescriptors} that reads descriptors from the
 * passed filesystem.  It expects descriptors to be in a file in the
 * {@link #TABLEINFO_DIR} subdir of the table's directory in FS.  Can be read-only
 *  -- i.e. does not modify the filesystem or can be read and write.
 *
 * <p>Also has utility for keeping up the table descriptors tableinfo file.
 * The table schema file is kept in the {@link #TABLEINFO_DIR} subdir
 * of the table directory in the filesystem.
 * It has a {@link #TABLEINFO_FILE_PREFIX} and then a suffix that is the
 * edit sequenceid: e.g. <code>.tableinfo.0000000003</code>.  This sequenceid
 * is always increasing.  It starts at zero.  The table schema file with the
 * highest sequenceid has the most recent schema edit. Usually there is one file
 * only, the most recent but there may be short periods where there are more
 * than one file. Old files are eventually cleaned.  Presumption is that there
 * will not be lots of concurrent clients making table schema edits.  If so,
 * the below needs a bit of a reworking and perhaps some supporting api in hdfs.
 */
@InterfaceAudience.Private
public final class LegacyTableDescriptor {
  private static final Log LOG = LogFactory.getLog(LegacyTableDescriptor.class);

  /** The file name prefix used to store HTD in HDFS  */
  public static final String TABLEINFO_FILE_PREFIX = ".tableinfo";
  public static final String TABLEINFO_DIR = ".tabledesc";
  public static final String TMP_DIR = ".tmp";

  private LegacyTableDescriptor() {}

  /**
   * Find the most current table info file for the table located in the given table directory.
   *
   * Looks within the {@link #TABLEINFO_DIR} subdirectory of the given directory for any table info
   * files and takes the 'current' one - meaning the one with the highest sequence number if present
   * or no sequence number at all if none exist (for backward compatibility from before there
   * were sequence numbers).
   *
   * @return The file status of the current table info file or null if it does not exist
   * @throws IOException
   */
  public static FileStatus getTableInfoPath(FileSystem fs, Path tableDir) throws IOException {
    return getTableInfoPath(fs, tableDir, false);
  }

  /**
   * Find the most current table info file for the table in the given table directory.
   *
   * Looks within the {@link #TABLEINFO_DIR} subdirectory of the given directory for any table info
   * files and takes the 'current' one - meaning the one with the highest sequence number if
   * present or no sequence number at all if none exist (for backward compatibility from before
   * there were sequence numbers).
   * If there are multiple table info files found and removeOldFiles is true it also deletes the
   * older files.
   *
   * @return The file status of the current table info file or null if none exist
   * @throws IOException
   */
  private static FileStatus getTableInfoPath(FileSystem fs, Path tableDir, boolean removeOldFiles)
      throws IOException {
    Path tableInfoDir = new Path(tableDir, TABLEINFO_DIR);
    return getCurrentTableInfoStatus(fs, tableInfoDir, removeOldFiles);
  }

  /**
   * Find the most current table info file in the given directory
   *
   * Looks within the given directory for any table info files
   * and takes the 'current' one - meaning the one with the highest sequence number if present
   * or no sequence number at all if none exist (for backward compatibility from before there
   * were sequence numbers).
   * If there are multiple possible files found
   * and the we're not in read only mode it also deletes the older files.
   *
   * @return The file status of the current table info file or null if it does not exist
   * @throws IOException
   */
  // only visible for FSTableDescriptorMigrationToSubdir, can be removed with that
  public static FileStatus getCurrentTableInfoStatus(FileSystem fs, Path dir, boolean removeOldFiles)
      throws IOException {
    FileStatus [] status = FSUtils.listStatus(fs, dir, TABLEINFO_PATHFILTER);
    if (status == null || status.length < 1) return null;
    FileStatus mostCurrent = null;
    for (FileStatus file : status) {
      if (mostCurrent == null || TABLEINFO_FILESTATUS_COMPARATOR.compare(file, mostCurrent) < 0) {
        mostCurrent = file;
      }
    }
    if (removeOldFiles && status.length > 1) {
      // Clean away old versions
      for (FileStatus file : status) {
        Path path = file.getPath();
        if (file != mostCurrent) {
          if (!fs.delete(path, false)) {
            LOG.warn("Failed cleanup of " + path);
          } else {
            LOG.debug("Cleaned up old tableinfo file " + path);
          }
        }
      }
    }
    return mostCurrent;
  }

  /**
   * Update table descriptor on the file system
   * @throws IOException Thrown if failed update.
   * @throws NotImplementedException if in read only mode
   */
  public static void updateTableDescriptor(FileSystem fs, Path tableDir, HTableDescriptor td)
      throws IOException {
    TableName tableName = td.getTableName();
    Path p = writeTableDescriptor(fs, td, tableDir, getTableInfoPath(fs, tableDir));
    if (p == null) throw new IOException("Failed update");
    LOG.info("Updated tableinfo=" + p);
  }

  /**
   * Compare {@link FileStatus} instances by {@link Path#getName()}. Returns in
   * reverse order.
   */
  @VisibleForTesting
  public static final Comparator<FileStatus> TABLEINFO_FILESTATUS_COMPARATOR =
  new Comparator<FileStatus>() {
    @Override
    public int compare(FileStatus left, FileStatus right) {
      return right.compareTo(left);
    }};

  private static final PathFilter TABLEINFO_PATHFILTER = new PathFilter() {
    @Override
    public boolean accept(Path p) {
      // Accept any file that starts with TABLEINFO_NAME
      return p.getName().startsWith(TABLEINFO_FILE_PREFIX);
    }};

  /**
   * Width of the sequenceid that is a suffix on a tableinfo file.
   */
  @VisibleForTesting public static final int WIDTH_OF_SEQUENCE_ID = 10;

  /*
   * @param number Number to use as suffix.
   * @return Returns zero-prefixed decimal version of passed
   * number (Does absolute in case number is negative).
   */
  private static String formatTableInfoSequenceId(final int number) {
    byte [] b = new byte[WIDTH_OF_SEQUENCE_ID];
    int d = Math.abs(number);
    for (int i = b.length - 1; i >= 0; i--) {
      b[i] = (byte)((d % 10) + '0');
      d /= 10;
    }
    return Bytes.toString(b);
  }

  /**
   * Regex to eat up sequenceid suffix on a .tableinfo file.
   * Use regex because may encounter oldstyle .tableinfos where there is no
   * sequenceid on the end.
   */
  private static final Pattern TABLEINFO_FILE_REGEX =
    Pattern.compile(TABLEINFO_FILE_PREFIX + "(\\.([0-9]{" + WIDTH_OF_SEQUENCE_ID + "}))?$");

  /**
   * @param p Path to a <code>.tableinfo</code> file.
   * @return The current editid or 0 if none found.
   */
  @VisibleForTesting
  public static int getTableInfoSequenceId(final Path p) {
    if (p == null) return 0;
    Matcher m = TABLEINFO_FILE_REGEX.matcher(p.getName());
    if (!m.matches()) throw new IllegalArgumentException(p.toString());
    String suffix = m.group(2);
    if (suffix == null || suffix.length() <= 0) return 0;
    return Integer.parseInt(m.group(2));
  }

  /**
   * @param sequenceid
   * @return Name of tableinfo file.
   */
  @VisibleForTesting
  public static String getTableInfoFileName(final int sequenceid) {
    return TABLEINFO_FILE_PREFIX + "." + formatTableInfoSequenceId(sequenceid);
  }

  /**
   * Returns the latest table descriptor for the given table directly from the file system
   * if it exists, bypassing the local cache.
   * Returns null if it's not found.
   */
  public static HTableDescriptor getTableDescriptorFromFs(FileSystem fs,
      Path hbaseRootDir, TableName tableName) throws IOException {
    Path tableDir = FSUtils.getTableDir(hbaseRootDir, tableName);
    return readTableDescriptor(fs, tableDir);
  }

  /**
   * Returns the latest table descriptor for the table located at the given directory
   * directly from the file system if it exists.
   * @throws TableInfoMissingException if there is no descriptor
   */
  public static HTableDescriptor getTableDescriptorFromFs(FileSystem fs, Path tableDir)
      throws IOException {
    final HTableDescriptor htd = readTableDescriptor(fs, tableDir);
    if (htd == null) {
      throw new TableInfoMissingException("No table descriptor file under " + tableDir);
    }
    return htd;
  }

  /**
   * @return null if not found
   */
  private static HTableDescriptor readTableDescriptor(FileSystem fs, Path tableDir)
      throws IOException {
    FileStatus status = getTableInfoPath(fs, tableDir, false);
    if (status == null) {
      return null;
    }
    return readTableDescriptor(fs, status);
  }

  private static HTableDescriptor readTableDescriptor(FileSystem fs, FileStatus status)
      throws IOException {
    int len = Ints.checkedCast(status.getLen());
    byte[] content = new byte[len];
    FSUtils.readFully(fs, status.getPath(), content);
    try {
      HTableDescriptor td = HTableDescriptor.parseFrom(content);
      return td;
    } catch (DeserializationException exception) {
      throw new IOException("failed to deserialize descriptor.", exception);
    }
  }

  /**
   * Deletes files matching the table info file pattern within the given directory
   * whose sequenceId is at most the given max sequenceId.
   */
  private static void deleteTableDescriptorFiles(FileSystem fs, Path dir, int maxSequenceId)
      throws IOException {
    FileStatus [] status = FSUtils.listStatus(fs, dir, TABLEINFO_PATHFILTER);
    for (FileStatus file : status) {
      Path path = file.getPath();
      int sequenceId = getTableInfoSequenceId(path);
      if (sequenceId <= maxSequenceId) {
        boolean success = FSUtils.delete(fs, path, false);
        if (success) {
          LOG.debug("Deleted table descriptor at " + path);
        } else {
          LOG.error("Failed to delete descriptor at " + path);
        }
      }
    }
  }

  /**
   * Attempts to write a new table descriptor to the given table's directory.
   * It first writes it to the .tmp dir then uses an atomic rename to move it into place.
   * It begins at the currentSequenceId + 1 and tries 10 times to find a new sequence number
   * not already in use.
   * Removes the current descriptor file if passed in.
   *
   * @return Descriptor file or null if we failed write.
   */
  private static Path writeTableDescriptor(final FileSystem fs,
    final HTableDescriptor htd, final Path tableDir,
    final FileStatus currentDescriptorFile) throws IOException {
    // Get temporary dir into which we'll first write a file to avoid half-written file phenomenon.
    // This directory is never removed to avoid removing it out from under a concurrent writer.
    Path tmpTableDir = new Path(tableDir, TMP_DIR);
    Path tableInfoDir = new Path(tableDir, TABLEINFO_DIR);

    // What is current sequenceid?  We read the current sequenceid from
    // the current file.  After we read it, another thread could come in and
    // compete with us writing out next version of file.  The below retries
    // should help in this case some but its hard to do guarantees in face of
    // concurrent schema edits.
    int currentSequenceId = currentDescriptorFile == null ? 0 :
      getTableInfoSequenceId(currentDescriptorFile.getPath());
    int newSequenceId = currentSequenceId;

    // Put arbitrary upperbound on how often we retry
    int retries = 10;
    int retrymax = currentSequenceId + retries;
    Path tableInfoDirPath = null;
    do {
      newSequenceId += 1;
      String filename = getTableInfoFileName(newSequenceId);
      Path tempPath = new Path(tmpTableDir, filename);
      if (fs.exists(tempPath)) {
        LOG.debug(tempPath + " exists; retrying up to " + retries + " times");
        continue;
      }
      tableInfoDirPath = new Path(tableInfoDir, filename);
      try {
        writeTD(fs, tempPath, htd);
        fs.mkdirs(tableInfoDirPath.getParent());
        if (!fs.rename(tempPath, tableInfoDirPath)) {
          throw new IOException("Failed rename of " + tempPath + " to " + tableInfoDirPath);
        }
        LOG.debug("Wrote descriptor into: " + tableInfoDirPath);
      } catch (IOException ioe) {
        // Presume clash of names or something; go around again.
        LOG.debug("Failed write and/or rename; retrying", ioe);
        if (!FSUtils.deleteDirectory(fs, tempPath)) {
          LOG.warn("Failed cleanup of " + tempPath);
        }
        tableInfoDirPath = null;
        continue;
      }
      break;
    } while (newSequenceId < retrymax);
    if (tableInfoDirPath != null) {
      // if we succeeded, remove old table info files.
      deleteTableDescriptorFiles(fs, tableInfoDir, newSequenceId - 1);
    }
    return tableInfoDirPath;
  }

  private static void writeTD(final FileSystem fs, final Path p, final HTableDescriptor htd)
      throws IOException {
    // We used to write this file out as a serialized HTD Writable followed by two '\n's and then
    // the toString version of HTD.  Now we just write out the pb serialization.
    FSUtils.writeFully(fs, p, htd.toByteArray(), false);
  }

  /**
   * Create a new HTableDescriptor in HDFS in the specified table directory. Happens when we create
   * a new table or snapshot a table.
   * @param tableDir table directory under which we should write the file
   * @param htd description of the table to write
   * @param forceCreation if <tt>true</tt>,then even if previous table descriptor is present it will
   *          be overwritten
   * @return <tt>true</tt> if the we successfully created the file, <tt>false</tt> if the file
   *         already exists and we weren't forcing the descriptor creation.
   * @throws IOException if a filesystem error occurs
   */
  public static boolean createTableDescriptor(FileSystem fs, Path tableDir,
      HTableDescriptor htd, boolean forceCreation) throws IOException {
    FileStatus status = getTableInfoPath(fs, tableDir);
    if (status != null) {
      LOG.debug("Current tableInfoPath = " + status.getPath());
      if (!forceCreation) {
        if (fs.exists(status.getPath()) && status.getLen() > 0) {
          if (readTableDescriptor(fs, status).equals(htd)) {
            LOG.debug("TableInfo already exists.. Skipping creation");
            return false;
          }
        }
      }
    }
    Path p = writeTableDescriptor(fs, htd, tableDir, status);
    return p != null;
  }
}
