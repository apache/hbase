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
package org.apache.hadoop.hbase.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableInfoMissingException;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;


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
public class FSTableDescriptors implements TableDescriptors {
  private static final Log LOG = LogFactory.getLog(FSTableDescriptors.class);
  private final FileSystem fs;
  private final Path rootdir;
  private final boolean fsreadonly;
  private volatile boolean usecache;
  private volatile boolean fsvisited;

  @VisibleForTesting long cachehits = 0;
  @VisibleForTesting long invocations = 0;

  /** The file name prefix used to store HTD in HDFS  */
  static final String TABLEINFO_FILE_PREFIX = ".tableinfo";
  static final String TABLEINFO_DIR = ".tabledesc";
  static final String TMP_DIR = ".tmp";

  // This cache does not age out the old stuff.  Thinking is that the amount
  // of data we keep up in here is so small, no need to do occasional purge.
  // TODO.
  private final Map<TableName, HTableDescriptor> cache =
    new ConcurrentHashMap<TableName, HTableDescriptor>();

  /**
   * Table descriptor for <code>hbase:meta</code> catalog table
   */
   private final HTableDescriptor metaTableDescriptor;

   /**
   * Construct a FSTableDescriptors instance using the hbase root dir of the given
   * conf and the filesystem where that root dir lives.
   * This instance can do write operations (is not read only).
   */
  public FSTableDescriptors(final Configuration conf) throws IOException {
    this(conf, FSUtils.getCurrentFileSystem(conf), FSUtils.getRootDir(conf));
  }

  public FSTableDescriptors(final Configuration conf, final FileSystem fs, final Path rootdir)
      throws IOException {
    this(conf, fs, rootdir, false, true);
  }

  /**
   * @param fsreadonly True if we are read-only when it comes to filesystem
   * operations; i.e. on remove, we do not do delete in fs.
   */
  public FSTableDescriptors(final Configuration conf, final FileSystem fs,
    final Path rootdir, final boolean fsreadonly, final boolean usecache) throws IOException {
    super();
    this.fs = fs;
    this.rootdir = rootdir;
    this.fsreadonly = fsreadonly;
    this.usecache = usecache;
    this.metaTableDescriptor = HTableDescriptor.metaTableDescriptor(conf);
  }

  public void setCacheOn() throws IOException {
    this.cache.clear();
    this.usecache = true;
  }

  public void setCacheOff() throws IOException {
    this.usecache = false;
    this.cache.clear();
  }

  @VisibleForTesting
  public boolean isUsecache() {
    return this.usecache;
  }

  /**
   * Get the current table descriptor for the given table, or null if none exists.
   *
   * Uses a local cache of the descriptor but still checks the filesystem on each call
   * to see if a newer file has been created since the cached one was read.
   */
  @Override
  public HTableDescriptor get(final TableName tablename)
  throws IOException {
    invocations++;
    if (TableName.META_TABLE_NAME.equals(tablename)) {
      cachehits++;
      return metaTableDescriptor;
    }
    // hbase:meta is already handled. If some one tries to get the descriptor for
    // .logs, .oldlogs or .corrupt throw an exception.
    if (HConstants.HBASE_NON_USER_TABLE_DIRS.contains(tablename.getNameAsString())) {
       throw new IOException("No descriptor found for non table = " + tablename);
    }

    if (usecache) {
      // Look in cache of descriptors.
      HTableDescriptor cachedtdm = this.cache.get(tablename);
      if (cachedtdm != null) {
        cachehits++;
        return cachedtdm;
      }
    }
    HTableDescriptor tdmt = null;
    try {
      tdmt = getTableDescriptorFromFs(fs, rootdir, tablename, !fsreadonly);
    } catch (NullPointerException e) {
      LOG.debug("Exception during readTableDecriptor. Current table name = "
          + tablename, e);
    } catch (IOException ioe) {
      LOG.debug("Exception during readTableDecriptor. Current table name = "
          + tablename, ioe);
    }
    // last HTD written wins
    if (usecache && tdmt != null) {
      this.cache.put(tablename, tdmt);
    }

    return tdmt;
  }

  /**
   * Returns a map from table name to table descriptor for all tables.
   */
  @Override
  public Map<String, HTableDescriptor> getAll()
  throws IOException {
    Map<String, HTableDescriptor> htds = new TreeMap<String, HTableDescriptor>();

    if (fsvisited && usecache) {
      for (Map.Entry<TableName, HTableDescriptor> entry: this.cache.entrySet()) {
        htds.put(entry.getKey().toString(), entry.getValue());
      }
      // add hbase:meta to the response
      htds.put(HTableDescriptor.META_TABLEDESC.getTableName().getNameAsString(),
        HTableDescriptor.META_TABLEDESC);
    } else {
      LOG.debug("Fetching table descriptors from the filesystem.");
      boolean allvisited = true;
      for (Path d : FSUtils.getTableDirs(fs, rootdir)) {
        HTableDescriptor htd = null;
        try {
          htd = get(FSUtils.getTableName(d));
        } catch (FileNotFoundException fnfe) {
          // inability of retrieving one HTD shouldn't stop getting the remaining
          LOG.warn("Trouble retrieving htd", fnfe);
        }
        if (htd == null) {
          allvisited = false;
          continue;
        } else {
          htds.put(htd.getTableName().getNameAsString(), htd);
        }
        fsvisited = allvisited;
      }
    }
    return htds;
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hbase.TableDescriptors#getTableDescriptors(org.apache.hadoop.fs.FileSystem, org.apache.hadoop.fs.Path)
   */
  @Override
  public Map<String, HTableDescriptor> getByNamespace(String name)
  throws IOException {
    Map<String, HTableDescriptor> htds = new TreeMap<String, HTableDescriptor>();
    List<Path> tableDirs =
        FSUtils.getLocalTableDirs(fs, FSUtils.getNamespaceDir(rootdir, name));
    for (Path d: tableDirs) {
      HTableDescriptor htd = null;
      try {
        htd = get(FSUtils.getTableName(d));
      } catch (FileNotFoundException fnfe) {
        // inability of retrieving one HTD shouldn't stop getting the remaining
        LOG.warn("Trouble retrieving htd", fnfe);
      }
      if (htd == null) continue;
      htds.put(FSUtils.getTableName(d).getNameAsString(), htd);
    }
    return htds;
  }

  /**
   * Adds (or updates) the table descriptor to the FileSystem
   * and updates the local cache with it.
   */
  @Override
  public void add(HTableDescriptor htd) throws IOException {
    if (fsreadonly) {
      throw new NotImplementedException("Cannot add a table descriptor - in read only mode");
    }
    if (TableName.META_TABLE_NAME.equals(htd.getTableName())) {
      throw new NotImplementedException();
    }
    if (HConstants.HBASE_NON_USER_TABLE_DIRS.contains(htd.getTableName().getNameAsString())) {
      throw new NotImplementedException(
        "Cannot add a table descriptor for a reserved subdirectory name: " + htd.getNameAsString());
    }
    updateTableDescriptor(htd);
  }

  /**
   * Removes the table descriptor from the local cache and returns it.
   * If not in read only mode, it also deletes the entire table directory(!)
   * from the FileSystem.
   */
  @Override
  public HTableDescriptor remove(final TableName tablename)
  throws IOException {
    if (fsreadonly) {
      throw new NotImplementedException("Cannot remove a table descriptor - in read only mode");
    }
    Path tabledir = getTableDir(tablename);
    if (this.fs.exists(tabledir)) {
      if (!this.fs.delete(tabledir, true)) {
        throw new IOException("Failed delete of " + tabledir.toString());
      }
    }
    HTableDescriptor descriptor = this.cache.remove(tablename);
    if (descriptor == null) {
      return null;
    } else {
      return descriptor;
    }
  }

  /**
   * Checks if a current table info file exists for the given table
   *
   * @param tableName name of table
   * @return true if exists
   * @throws IOException
   */
  public boolean isTableInfoExists(TableName tableName) throws IOException {
    return getTableInfoPath(tableName) != null;
  }

  /**
   * Find the most current table info file for the given table in the hbase root directory.
   * @return The file status of the current table info file or null if it does not exist
   */
  private FileStatus getTableInfoPath(final TableName tableName) throws IOException {
    Path tableDir = getTableDir(tableName);
    return getTableInfoPath(tableDir);
  }

  private FileStatus getTableInfoPath(Path tableDir)
  throws IOException {
    return getTableInfoPath(fs, tableDir, !fsreadonly);
  }

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
  public static FileStatus getTableInfoPath(FileSystem fs, Path tableDir)
  throws IOException {
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
  static FileStatus getCurrentTableInfoStatus(FileSystem fs, Path dir, boolean removeOldFiles)
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
          if (!fs.delete(file.getPath(), false)) {
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
   * Compare {@link FileStatus} instances by {@link Path#getName()}. Returns in
   * reverse order.
   */
  @VisibleForTesting
  static final Comparator<FileStatus> TABLEINFO_FILESTATUS_COMPARATOR =
  new Comparator<FileStatus>() {
    @Override
    public int compare(FileStatus left, FileStatus right) {
      return right.compareTo(left);
    }};

  /**
   * Return the table directory in HDFS
   */
  @VisibleForTesting Path getTableDir(final TableName tableName) {
    return FSUtils.getTableDir(rootdir, tableName);
  }

  private static final PathFilter TABLEINFO_PATHFILTER = new PathFilter() {
    @Override
    public boolean accept(Path p) {
      // Accept any file that starts with TABLEINFO_NAME
      return p.getName().startsWith(TABLEINFO_FILE_PREFIX);
    }};

  /**
   * Width of the sequenceid that is a suffix on a tableinfo file.
   */
  @VisibleForTesting static final int WIDTH_OF_SEQUENCE_ID = 10;

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
  @VisibleForTesting static int getTableInfoSequenceId(final Path p) {
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
  @VisibleForTesting static String getTableInfoFileName(final int sequenceid) {
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
    return getTableDescriptorFromFs(fs, tableDir);
  }

  /**
   * Returns the latest table descriptor for the table located at the given directory
   * directly from the file system if it exists.
   * @throws TableInfoMissingException if there is no descriptor
   */
  public static HTableDescriptor getTableDescriptorFromFs(FileSystem fs,
    Path hbaseRootDir, TableName tableName, boolean rewritePb) throws IOException {
    Path tableDir = FSUtils.getTableDir(hbaseRootDir, tableName);
    return getTableDescriptorFromFs(fs, tableDir, rewritePb);
  }
  /**
   * Returns the latest table descriptor for the table located at the given directory
   * directly from the file system if it exists.
   * @throws TableInfoMissingException if there is no descriptor
   */
  public static HTableDescriptor getTableDescriptorFromFs(FileSystem fs, Path tableDir)
    throws IOException {
    return getTableDescriptorFromFs(fs, tableDir, false);
  }

  /**
   * Returns the latest table descriptor for the table located at the given directory
   * directly from the file system if it exists.
   * @throws TableInfoMissingException if there is no descriptor
   */
  public static HTableDescriptor getTableDescriptorFromFs(FileSystem fs, Path tableDir,
    boolean rewritePb)
  throws IOException {
    FileStatus status = getTableInfoPath(fs, tableDir, false);
    if (status == null) {
      throw new TableInfoMissingException("No table descriptor file under " + tableDir);
    }
    return readTableDescriptor(fs, status, rewritePb);
  }

  private static HTableDescriptor readTableDescriptor(FileSystem fs, FileStatus status,
      boolean rewritePb) throws IOException {
    int len = Ints.checkedCast(status.getLen());
    byte [] content = new byte[len];
    FSDataInputStream fsDataInputStream = fs.open(status.getPath());
    try {
      fsDataInputStream.readFully(content);
    } finally {
      fsDataInputStream.close();
    }
    HTableDescriptor htd = null;
    try {
      htd = HTableDescriptor.parseFrom(content);
    } catch (DeserializationException e) {
      // we have old HTableDescriptor here
      try {
        HTableDescriptor ohtd = HTableDescriptor.parseFrom(content);
        LOG.warn("Found old table descriptor, converting to new format for table " +
          ohtd.getTableName());
        htd = new HTableDescriptor(ohtd);
        if (rewritePb) rewriteTableDescriptor(fs, status, htd);
      } catch (DeserializationException e1) {
        throw new IOException("content=" + Bytes.toShort(content), e1);
      }
    }
    if (rewritePb && !ProtobufUtil.isPBMagicPrefix(content)) {
      // Convert the file over to be pb before leaving here.
      rewriteTableDescriptor(fs, status, htd);
    }
    return htd;
  }

  private static void rewriteTableDescriptor(final FileSystem fs, final FileStatus status,
    final HTableDescriptor td)
  throws IOException {
    Path tableInfoDir = status.getPath().getParent();
    Path tableDir = tableInfoDir.getParent();
    writeTableDescriptor(fs, td, tableDir, status);
  }

  /**
   * Update table descriptor on the file system
   * @throws IOException Thrown if failed update.
   * @throws NotImplementedException if in read only mode
   */
  @VisibleForTesting Path updateTableDescriptor(HTableDescriptor htd)
  throws IOException {
    if (fsreadonly) {
      throw new NotImplementedException("Cannot update a table descriptor - in read only mode");
    }
    Path tableDir = getTableDir(htd.getTableName());
    Path p = writeTableDescriptor(fs, htd, tableDir, getTableInfoPath(tableDir));
    if (p == null) throw new IOException("Failed update");
    LOG.info("Updated tableinfo=" + p);
    if (usecache) {
      this.cache.put(htd.getTableName(), htd);
    }
    return p;
  }

  /**
   * Deletes all the table descriptor files from the file system.
   * Used in unit tests only.
   * @throws NotImplementedException if in read only mode
   */
  public void deleteTableDescriptorIfExists(TableName tableName) throws IOException {
    if (fsreadonly) {
      throw new NotImplementedException("Cannot delete a table descriptor - in read only mode");
    }

    Path tableDir = getTableDir(tableName);
    Path tableInfoDir = new Path(tableDir, TABLEINFO_DIR);
    deleteTableDescriptorFiles(fs, tableInfoDir, Integer.MAX_VALUE);
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
    final FileStatus currentDescriptorFile)
  throws IOException {
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
        writeHTD(fs, tempPath, htd);
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

  private static void writeHTD(final FileSystem fs, final Path p, final HTableDescriptor htd)
  throws IOException {
    FSDataOutputStream out = fs.create(p, false);
    try {
      // We used to write this file out as a serialized HTD Writable followed by two '\n's and then
      // the toString version of HTD.  Now we just write out the pb serialization.
      out.write(htd.toByteArray());
    } finally {
      out.close();
    }
  }

  /**
   * Create new HTableDescriptor in HDFS. Happens when we are creating table.
   * Used by tests.
   * @return True if we successfully created file.
   */
  public boolean createTableDescriptor(HTableDescriptor htd) throws IOException {
    return createTableDescriptor(htd, false);
  }

  /**
   * Create new HTableDescriptor in HDFS. Happens when we are creating table. If
   * forceCreation is true then even if previous table descriptor is present it
   * will be overwritten
   *
   * @return True if we successfully created file.
   */
  public boolean createTableDescriptor(HTableDescriptor htd, boolean forceCreation)
  throws IOException {
    Path tableDir = getTableDir(htd.getTableName());
    return createTableDescriptorForTableDirectory(tableDir, htd, forceCreation);
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
  public boolean createTableDescriptorForTableDirectory(Path tableDir,
      HTableDescriptor htd, boolean forceCreation) throws IOException {
    if (fsreadonly) {
      throw new NotImplementedException("Cannot create a table descriptor - in read only mode");
    }
    FileStatus status = getTableInfoPath(fs, tableDir);
    if (status != null) {
      LOG.debug("Current tableInfoPath = " + status.getPath());
      if (!forceCreation) {
        if (fs.exists(status.getPath()) && status.getLen() > 0) {
          if (readTableDescriptor(fs, status, false).equals(htd)) {
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

