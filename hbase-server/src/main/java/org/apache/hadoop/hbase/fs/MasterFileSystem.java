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

package org.apache.hadoop.hbase.fs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.ClusterId;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.fs.legacy.LegacyMasterFileSystem;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;

import org.apache.hadoop.hbase.backup.HFileArchiver;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobUtils;

@InterfaceAudience.Private
public abstract class MasterFileSystem {
  private static Log LOG = LogFactory.getLog(MasterFileSystem.class);

  // Persisted unique cluster ID
  private ClusterId clusterId;


  private Configuration conf;
  private FileSystem fs;
  private Path rootDir;

  protected MasterFileSystem(Configuration conf, FileSystem fs, Path rootDir) {
    this.rootDir = rootDir;
    this.conf = conf;
    this.fs = fs;
  }

  public Configuration getConfiguration() { return conf; }
  public FileSystem getFileSystem() { return fs; }
  public Path getRootDir() { return rootDir; }

  // ==========================================================================
  //  PUBLIC Interfaces - Visitors
  // ==========================================================================
  public interface NamespaceVisitor {
    void visitNamespace(String namespace) throws IOException;
  }

  public interface TableVisitor {
    void visitTable(TableName tableName) throws IOException;
  }

  public interface RegionVisitor {
    void visitRegion(HRegionInfo regionInfo) throws IOException;
  }

  // ==========================================================================
  //  PUBLIC Methods - Namespace related
  // ==========================================================================
  public abstract void createNamespace(NamespaceDescriptor nsDescriptor) throws IOException;
  public abstract void deleteNamespace(String namespaceName) throws IOException;
  public abstract Collection<String> getNamespaces(FsContext ctx) throws IOException;

  public Collection<String> getNamespaces() throws IOException {
    return getNamespaces(FsContext.DATA);
  }
  // should return or get a NamespaceDescriptor? how is that different from HTD?

  // ==========================================================================
  //  PUBLIC Methods - Table Descriptor related
  // ==========================================================================
  public HTableDescriptor getTableDescriptor(TableName tableName)
      throws IOException {
    return getTableDescriptor(FsContext.DATA, tableName);
  }

  public boolean createTableDescriptor(HTableDescriptor tableDesc, boolean force)
      throws IOException {
    return createTableDescriptor(FsContext.DATA, tableDesc, force);
  }

  public void updateTableDescriptor(HTableDescriptor tableDesc) throws IOException {
    updateTableDescriptor(FsContext.DATA, tableDesc);
  }

  public abstract HTableDescriptor getTableDescriptor(FsContext ctx, TableName tableName)
      throws IOException;
  public abstract boolean createTableDescriptor(FsContext ctx, HTableDescriptor tableDesc,
      boolean force) throws IOException;
  public abstract void updateTableDescriptor(FsContext ctx, HTableDescriptor tableDesc)
      throws IOException;

  // ==========================================================================
  //  PUBLIC Methods - Table related
  // ==========================================================================
  public void deleteTable(TableName tableName) throws IOException {
    deleteTable(FsContext.DATA, tableName);
  }

  public Collection<TableName> getTables(String namespace) throws IOException {
    return getTables(FsContext.DATA, namespace);
  }

  public abstract void deleteTable(FsContext ctx, TableName tableName) throws IOException;

  public abstract Collection<TableName> getTables(FsContext ctx, String namespace)
    throws IOException;

  public Collection<TableName> getTables() throws IOException {
    ArrayList<TableName> tables = new ArrayList<TableName>();
    for (String ns: getNamespaces()) {
      tables.addAll(getTables(ns));
    }
    return tables;
  }

  // ==========================================================================
  //  PUBLIC Methods - bootstrap
  // ==========================================================================
  public abstract Path getTempDir();

  public void logFileSystemState(Log log) throws IOException {
    FSUtils.logFileSystemState(getFileSystem(), getRootDir(), LOG);
  }

  /**
   * @return The unique identifier generated for this cluster
   */
  public ClusterId getClusterId() {
    return clusterId;
  }

  protected void bootstrap() throws IOException {
    // check if the root directory exists
    createInitialLayout(getRootDir(), conf, this.fs);

    // check if temp directory exists and clean it
    startupCleanup();
  }

  protected abstract void bootstrapMeta() throws IOException;
  protected abstract void startupCleanup() throws IOException;

  /**
   * Create initial layout in filesystem.
   * <ol>
   * <li>Check if the meta region exists and is readable, if not create it.
   * Create hbase.version and the hbase:meta directory if not one.
   * </li>
   * <li>Create a log archive directory for RS to put archived logs</li>
   * </ol>
   * Idempotent.
   */
  private void createInitialLayout(final Path rd, final Configuration c, final FileSystem fs)
      throws IOException {
    // If FS is in safe mode wait till out of it.
    FSUtils.waitOnSafeMode(c, c.getInt(HConstants.THREAD_WAKE_FREQUENCY, 10 * 1000));

    // Filesystem is good. Go ahead and check for hbase.rootdir.
    try {
      if (!fs.exists(rd)) {
        fs.mkdirs(rd);
        // DFS leaves safe mode with 0 DNs when there are 0 blocks.
        // We used to handle this by checking the current DN count and waiting until
        // it is nonzero. With security, the check for datanode count doesn't work --
        // it is a privileged op. So instead we adopt the strategy of the jobtracker
        // and simply retry file creation during bootstrap indefinitely. As soon as
        // there is one datanode it will succeed. Permission problems should have
        // already been caught by mkdirs above.
        FSUtils.setVersion(fs, rd, c.getInt(HConstants.THREAD_WAKE_FREQUENCY,
          10 * 1000), c.getInt(HConstants.VERSION_FILE_WRITE_ATTEMPTS,
            HConstants.DEFAULT_VERSION_FILE_WRITE_ATTEMPTS));
      } else {
        if (!fs.isDirectory(rd)) {
          throw new IllegalArgumentException(rd.toString() + " is not a directory");
        }
        // as above
        FSUtils.checkVersion(fs, rd, true, c.getInt(HConstants.THREAD_WAKE_FREQUENCY,
          10 * 1000), c.getInt(HConstants.VERSION_FILE_WRITE_ATTEMPTS,
            HConstants.DEFAULT_VERSION_FILE_WRITE_ATTEMPTS));
      }
    } catch (DeserializationException de) {
      LOG.fatal("Please fix invalid configuration for " + HConstants.HBASE_DIR, de);
      IOException ioe = new IOException();
      ioe.initCause(de);
      throw ioe;
    } catch (IllegalArgumentException iae) {
      LOG.fatal("Please fix invalid configuration for "
        + HConstants.HBASE_DIR + " " + rd.toString(), iae);
      throw iae;
    }
    // Make sure cluster ID exists
    if (!FSUtils.checkClusterIdExists(fs, rd, c.getInt(
        HConstants.THREAD_WAKE_FREQUENCY, 10 * 1000))) {
      FSUtils.setClusterId(fs, rd, new ClusterId(), c.getInt(HConstants.THREAD_WAKE_FREQUENCY, 10 * 1000));
    }
    clusterId = FSUtils.getClusterId(fs, rd);

    // Make sure the meta region exists!
    bootstrapMeta();
  }

  // TODO: Move in HRegionFileSystem
  public void deleteFamilyFromFS(HRegionInfo region, byte[] familyName, boolean hasMob)
      throws IOException {
    // archive family store files
    Path tableDir = FSUtils.getTableDir(getRootDir(), region.getTable());
    HFileArchiver.archiveFamily(fs, conf, region, tableDir, familyName);

    // delete the family folder
    Path familyDir = new Path(tableDir,
      new Path(region.getEncodedName(), Bytes.toString(familyName)));
    if (fs.delete(familyDir, true) == false) {
      if (fs.exists(familyDir)) {
        throw new IOException("Could not delete family "
            + Bytes.toString(familyName) + " from FileSystem for region "
            + region.getRegionNameAsString() + "(" + region.getEncodedName()
            + ")");
      }
    }

    // archive and delete mob files
    if (hasMob) {
      Path mobTableDir =
          FSUtils.getTableDir(new Path(getRootDir(), MobConstants.MOB_DIR_NAME), region.getTable());
      HRegionInfo mobRegionInfo = MobUtils.getMobRegionInfo(region.getTable());
      Path mobFamilyDir =
          new Path(mobTableDir,
              new Path(mobRegionInfo.getEncodedName(), Bytes.toString(familyName)));
      // archive mob family store files
      MobUtils.archiveMobStoreFiles(conf, fs, mobRegionInfo, mobFamilyDir, familyName);

      if (!fs.delete(mobFamilyDir, true)) {
        throw new IOException("Could not delete mob store files for family "
            + Bytes.toString(familyName) + " from FileSystem region "
            + mobRegionInfo.getRegionNameAsString() + "(" + mobRegionInfo.getEncodedName() + ")");
      }
    }
  }

  // ==========================================================================
  //  PUBLIC
  // ==========================================================================
  public static MasterFileSystem open(Configuration conf, boolean bootstrap)
      throws IOException {
    return open(conf, FSUtils.getCurrentFileSystem(conf), FSUtils.getRootDir(conf), bootstrap);
  }

  public static MasterFileSystem open(Configuration conf, FileSystem fs,
      Path rootDir, boolean bootstrap) throws IOException {
    // Cover both bases, the old way of setting default fs and the new.
    // We're supposed to run on 0.20 and 0.21 anyways.
    fs = rootDir.getFileSystem(conf);
    FSUtils.setFsDefault(conf, new Path(fs.getUri()));
    // make sure the fs has the same conf
    fs.setConf(conf);

    MasterFileSystem mfs = getInstance(conf, fs, rootDir);
    if (bootstrap) {
      mfs.bootstrap();
    }
    HFileSystem.addLocationsOrderInterceptor(conf);
    return mfs;
  }

  private static MasterFileSystem getInstance(Configuration conf, final FileSystem fs,
      Path rootDir) throws IOException {
    String fsType = conf.get("hbase.fs.layout.type", "legacy").toLowerCase();
    switch (fsType) {
      case "legacy":
        return new LegacyMasterFileSystem(conf, fs, rootDir);
      default:
        throw new IOException("Invalid filesystem type " + fsType);
    }
  }
}
