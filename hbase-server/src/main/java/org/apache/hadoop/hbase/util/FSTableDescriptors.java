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
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.annotations.VisibleForTesting;
import edu.umd.cs.findbugs.annotations.Nullable;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableInfoMissingException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.fs.MasterFileSystem;

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

  private final MasterFileSystem mfs;
  private final boolean fsreadonly;

  private volatile boolean usecache;
  private volatile boolean fsvisited;

  @VisibleForTesting long cachehits = 0;
  @VisibleForTesting long invocations = 0;

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
    this(MasterFileSystem.open(conf, fs, rootdir, false), fsreadonly, usecache);
  }

  private FSTableDescriptors(final MasterFileSystem mfs, boolean fsreadonly, boolean usecache)
      throws IOException {
    super();
    this.mfs = mfs;
    this.fsreadonly = fsreadonly;
    this.usecache = usecache;

    this.metaTableDescriptor = HTableDescriptor.metaTableDescriptor(mfs.getConfiguration());
  }

  @Override
  public void setCacheOn() throws IOException {
    this.cache.clear();
    this.usecache = true;
  }

  @Override
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
  @Nullable
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
      tdmt = mfs.getTableDescriptor(tablename);
    } catch (NullPointerException e) {
      LOG.debug("Exception during readTableDecriptor. Current table name = " + tablename, e);
    } catch (TableInfoMissingException e) {
      // ignore. This is regular operation
    } catch (IOException ioe) {
      LOG.debug("Exception during readTableDecriptor. Current table name = " + tablename, ioe);
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
  public Map<String, HTableDescriptor> getAllDescriptors()
  throws IOException {
    Map<String, HTableDescriptor> tds = new TreeMap<String, HTableDescriptor>();

    if (fsvisited && usecache) {
      for (Map.Entry<TableName, HTableDescriptor> entry: this.cache.entrySet()) {
        tds.put(entry.getKey().toString(), entry.getValue());
      }
      // add hbase:meta to the response
      tds.put(this.metaTableDescriptor.getNameAsString(), metaTableDescriptor);
    } else {
      LOG.debug("Fetching table descriptors from the filesystem.");
      boolean allvisited = true;

      for (TableName table: mfs.getTables()) {
        HTableDescriptor htd = null;
        try {
          htd = get(table);
        } catch (FileNotFoundException fnfe) {
          // inability of retrieving one HTD shouldn't stop getting the remaining
          LOG.warn("Trouble retrieving htd", fnfe);
        }
        if (htd == null) {
          allvisited = false;
          continue;
        } else {
          tds.put(htd.getTableName().getNameAsString(), htd);
        }
        fsvisited = allvisited;
      }
    }
    return tds;
  }

  /**
   * Returns a map from table name to table descriptor for all tables.
   */
  @Override
  public Map<String, HTableDescriptor> getAll() throws IOException {
    Map<String, HTableDescriptor> htds = new TreeMap<String, HTableDescriptor>();
    Map<String, HTableDescriptor> allDescriptors = getAllDescriptors();
    for (Map.Entry<String, HTableDescriptor> entry : allDescriptors
        .entrySet()) {
      htds.put(entry.getKey(), entry.getValue());
    }
    return htds;
  }

  /**
    * Find descriptors by namespace.
    * @see #get(org.apache.hadoop.hbase.TableName)
    */
  @Override
  public Map<String, HTableDescriptor> getByNamespace(String name) throws IOException {
    Map<String, HTableDescriptor> htds = new TreeMap<String, HTableDescriptor>();
    for (TableName table: mfs.getTables(name)) {
      HTableDescriptor htd = null;
      try {
        htd = get(table);
      } catch (FileNotFoundException fnfe) {
        // inability of retrieving one HTD shouldn't stop getting the remaining
        LOG.warn("Trouble retrieving htd", fnfe);
      }
      if (htd == null) continue;
      htds.put(table.getNameAsString(), htd);
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
    TableName tableName = htd.getTableName();
    if (TableName.META_TABLE_NAME.equals(tableName)) {
      throw new NotImplementedException();
    }
    if (HConstants.HBASE_NON_USER_TABLE_DIRS.contains(tableName.getNameAsString())) {
      throw new NotImplementedException(
          "Cannot add a table descriptor for a reserved subdirectory name: "
              + htd.getNameAsString());
    }
    updateTableDescriptor(htd);
  }

  /**
   * Removes the table descriptor from the local cache and returns it.
   * If not in read only mode, it also deletes the entire table directory(!)
   * from the FileSystem.
   */
  @Override
  public HTableDescriptor remove(final TableName tablename) throws IOException {
    if (fsreadonly) {
      throw new NotImplementedException("Cannot remove a table descriptor - in read only mode");
    }
    mfs.deleteTable(tablename); // for test only??
    HTableDescriptor descriptor = this.cache.remove(tablename);
    return descriptor;
  }

  /**
   * Update table descriptor on the file system
   * @throws IOException Thrown if failed update.
   * @throws NotImplementedException if in read only mode
   */
  @VisibleForTesting
  void updateTableDescriptor(HTableDescriptor td) throws IOException {
    if (fsreadonly) {
      throw new NotImplementedException("Cannot update a table descriptor - in read only mode");
    }
    mfs.updateTableDescriptor(td);
    if (usecache) {
      this.cache.put(td.getTableName(), td);
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
    if (fsreadonly) {
      throw new NotImplementedException("Cannot create a table descriptor - in read only mode");
    }

    // forceCreation???
    return mfs.createTableDescriptor(htd, forceCreation);
  }
}
