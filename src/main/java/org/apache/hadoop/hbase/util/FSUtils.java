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
package org.apache.hadoop.hbase.util;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HBaseFileSystem;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

/**
 * Utility methods for interacting with the underlying file system.
 */
public abstract class FSUtils {
  private static final Log LOG = LogFactory.getLog(FSUtils.class);

  /** Full access permissions (starting point for a umask) */
  private static final String FULL_RWX_PERMISSIONS = "777";

  protected FSUtils() {
    super();
  }

  public static FSUtils getInstance(FileSystem fs, Configuration conf) {
    String scheme = fs.getUri().getScheme();
    if (scheme == null) {
      LOG.warn("Could not find scheme for uri " +
          fs.getUri() + ", default to hdfs");
      scheme = "hdfs";
    }
    Class<?> fsUtilsClass = conf.getClass("hbase.fsutil." +
        scheme + ".impl", FSHDFSUtils.class); // Default to HDFS impl
    FSUtils fsUtils = (FSUtils)ReflectionUtils.newInstance(fsUtilsClass, conf);
    return fsUtils;
  }

  /**
   * Delete if exists.
   * @param fs filesystem object
   * @param dir directory to delete
   * @return True if deleted <code>dir</code>
   * @throws IOException e
   */
  public static boolean deleteDirectory(final FileSystem fs, final Path dir)
  throws IOException {
    return fs.exists(dir) && fs.delete(dir, true);
  }

  /**
   * Check if directory exists.  If it does not, create it.
   * @param fs filesystem object
   * @param dir path to check
   * @return Path
   * @throws IOException e
   */
  public Path checkdir(final FileSystem fs, final Path dir) throws IOException {
    if (!fs.exists(dir)) {
      HBaseFileSystem.makeDirOnFileSystem(fs, dir);
    }
    return dir;
  }

  /**
   * Create the specified file on the filesystem. By default, this will:
   * <ol>
   * <li>overwrite the file if it exists</li>
   * <li>apply the umask in the configuration (if it is enabled)</li>
   * <li>use the fs configured buffer size (or {@value DEFAULT_BUFFER_SIZE} if
   * not set)</li>
   * <li>use the default replication</li>
   * <li>use the default block size</li>
   * <li>not track progress</li>
   * </ol>
   *
   * @param fs {@link FileSystem} on which to write the file
   * @param path {@link Path} to the file to write
   * @return output stream to the created file
   * @throws IOException if the file cannot be created
   */
  public static FSDataOutputStream create(FileSystem fs, Path path,
      FsPermission perm) throws IOException {
    return create(fs, path, perm, true);
  }

  /**
   * Create the specified file on the filesystem. By default, this will:
   * <ol>
   * <li>apply the umask in the configuration (if it is enabled)</li>
   * <li>use the fs configured buffer size (or {@value DEFAULT_BUFFER_SIZE} if
   * not set)</li>
   * <li>use the default replication</li>
   * <li>use the default block size</li>
   * <li>not track progress</li>
   * </ol>
   *
   * @param fs {@link FileSystem} on which to write the file
   * @param path {@link Path} to the file to write
   * @param perm
   * @param overwrite Whether or not the created file should be overwritten.
   * @return output stream to the created file
   * @throws IOException if the file cannot be created
   */
  public static FSDataOutputStream create(FileSystem fs, Path path, FsPermission perm,
      boolean overwrite) throws IOException {
    LOG.debug("Creating file=" + path + " with permission=" + perm);
    return HBaseFileSystem.createPathWithPermsOnFileSystem(fs, path, perm, overwrite);
  }

  /**
   * Get the file permissions specified in the configuration, if they are
   * enabled.
   *
   * @param fs filesystem that the file will be created on.
   * @param conf configuration to read for determining if permissions are
   *          enabled and which to use
   * @param permssionConfKey property key in the configuration to use when
   *          finding the permission
   * @return the permission to use when creating a new file on the fs. If
   *         special permissions are not specified in the configuration, then
   *         the default permissions on the the fs will be returned.
   */
  public static FsPermission getFilePermissions(final FileSystem fs,
      final Configuration conf, final String permssionConfKey) {
    boolean enablePermissions = conf.getBoolean(
        HConstants.ENABLE_DATA_FILE_UMASK, false);

    if (enablePermissions) {
      try {
        FsPermission perm = new FsPermission(FULL_RWX_PERMISSIONS);
        // make sure that we have a mask, if not, go default.
        String mask = conf.get(permssionConfKey);
        if (mask == null)
          return FsPermission.getDefault();
        // appy the umask
        FsPermission umask = new FsPermission(mask);
        return perm.applyUMask(umask);
      } catch (IllegalArgumentException e) {
        LOG.warn(
            "Incorrect umask attempted to be created: "
                + conf.get(permssionConfKey)
                + ", using default file permissions.", e);
        return FsPermission.getDefault();
      }
    }
    return FsPermission.getDefault();
  }

  /**
   * Checks to see if the specified file system is available
   *
   * @param fs filesystem
   * @throws IOException e
   */
  public static void checkFileSystemAvailable(final FileSystem fs)
  throws IOException {
    if (!(fs instanceof DistributedFileSystem)) {
      return;
    }
    IOException exception = null;
    DistributedFileSystem dfs = (DistributedFileSystem) fs;
    try {
      if (dfs.exists(new Path("/"))) {
        return;
      }
    } catch (IOException e) {
      exception = RemoteExceptionHandler.checkIOException(e);
    }
    try {
      fs.close();
    } catch (Exception e) {
      LOG.error("file system close failed: ", e);
    }
    IOException io = new IOException("File system is not available");
    io.initCause(exception);
    throw io;
  }

  /**
   * We use reflection because {@link DistributedFileSystem#setSafeMode(
   * FSConstants.SafeModeAction action, boolean isChecked)} is not in hadoop 1.1
   * 
   * @param dfs
   * @return whether we're in safe mode
   * @throws IOException
   */
  private static boolean isInSafeMode(DistributedFileSystem dfs) throws IOException {
    boolean inSafeMode = false;
    try {
      Method m = DistributedFileSystem.class.getMethod("setSafeMode", new Class<?> []{
          org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction.class, boolean.class});
      inSafeMode = (Boolean) m.invoke(dfs,
        org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction.SAFEMODE_GET, true);
    } catch (Exception e) {
      if (e instanceof IOException) throw (IOException) e;
      
      // Check whether dfs is on safemode.
      inSafeMode = dfs.setSafeMode(
        org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction.SAFEMODE_GET);      
    }
    return inSafeMode;    
  }
  
  /**
   * Check whether dfs is in safemode.
   * @param conf
   * @throws IOException
   */
  public static void checkDfsSafeMode(final Configuration conf)
  throws IOException {
    boolean isInSafeMode = false;
    FileSystem fs = FileSystem.get(conf);
    if (fs instanceof DistributedFileSystem) {
      DistributedFileSystem dfs = (DistributedFileSystem)fs;
      isInSafeMode = isInSafeMode(dfs);
    }
    if (isInSafeMode) {
      throw new IOException("File system is in safemode, it can't be written now");
    }
  }

  /**
   * Verifies current version of file system
   *
   * @param fs filesystem object
   * @param rootdir root hbase directory
   * @return null if no version file exists, version string otherwise.
   * @throws IOException e
   */
  public static String getVersion(FileSystem fs, Path rootdir)
  throws IOException {
    Path versionFile = new Path(rootdir, HConstants.VERSION_FILE_NAME);
    String version = null;
    if (fs.exists(versionFile)) {
      FSDataInputStream s =
        fs.open(versionFile);
      try {
        version = DataInputStream.readUTF(s);
      } catch (EOFException eof) {
        LOG.warn("Version file was empty, odd, will try to set it.");
      } finally {
        s.close();
      }
    }
    return version;
  }

  /**
   * Verifies current version of file system
   *
   * @param fs file system
   * @param rootdir root directory of HBase installation
   * @param message if true, issues a message on System.out
   *
   * @throws IOException e
   */
  public static void checkVersion(FileSystem fs, Path rootdir,
      boolean message) throws IOException {
    checkVersion(fs, rootdir, message, 0,
    		HConstants.DEFAULT_VERSION_FILE_WRITE_ATTEMPTS);
  }

  /**
   * Verifies current version of file system
   *
   * @param fs file system
   * @param rootdir root directory of HBase installation
   * @param message if true, issues a message on System.out
   * @param wait wait interval
   * @param retries number of times to retry
   *
   * @throws IOException e
   */
  public static void checkVersion(FileSystem fs, Path rootdir,
      boolean message, int wait, int retries) throws IOException {
    String version = getVersion(fs, rootdir);

    if (version == null) {
      if (!rootRegionExists(fs, rootdir)) {
        // rootDir is empty (no version file and no root region)
        // just create new version file (HBASE-1195)
        FSUtils.setVersion(fs, rootdir, wait, retries);
        return;
      }
    } else if (version.compareTo(HConstants.FILE_SYSTEM_VERSION) == 0)
        return;

    // version is deprecated require migration
    // Output on stdout so user sees it in terminal.
    String msg = "HBase file layout needs to be upgraded."
      + "  You have version " + version
      + " and I want version " + HConstants.FILE_SYSTEM_VERSION
      + ".  Is your hbase.rootdir valid?  If so, you may need to run "
      + "'hbase hbck -fixVersionFile'.";
    if (message) {
      System.out.println("WARNING! " + msg);
    }
    throw new FileSystemVersionException(msg);
  }

  /**
   * Sets version of file system
   *
   * @param fs filesystem object
   * @param rootdir hbase root
   * @throws IOException e
   */
  public static void setVersion(FileSystem fs, Path rootdir)
  throws IOException {
    setVersion(fs, rootdir, HConstants.FILE_SYSTEM_VERSION, 0,
    		HConstants.DEFAULT_VERSION_FILE_WRITE_ATTEMPTS);
  }

  /**
   * Sets version of file system
   *
   * @param fs filesystem object
   * @param rootdir hbase root
   * @param wait time to wait for retry
   * @param retries number of times to retry before failing
   * @throws IOException e
   */
  public static void setVersion(FileSystem fs, Path rootdir, int wait, int retries)
  throws IOException {
    setVersion(fs, rootdir, HConstants.FILE_SYSTEM_VERSION, wait, retries);
  }

  /**
   * Return the number of bytes that large input files should be optimally
   * be split into to minimize i/o time.
   *
   * use reflection to search for getDefaultBlockSize(Path f)
   * if the method doesn't exist, fall back to using getDefaultBlockSize()
   *
   * @param fs filesystem object
   * @return the default block size for the path's filesystem
   * @throws IOException e
   */
  public static long getDefaultBlockSize(final FileSystem fs, final Path path) throws IOException {
    Method m = null;
    Class<? extends FileSystem> cls = fs.getClass();
    try {
      m = cls.getMethod("getDefaultBlockSize", new Class<?>[] { Path.class });
    } catch (NoSuchMethodException e) {
      LOG.info("FileSystem doesn't support getDefaultBlockSize");
    } catch (SecurityException e) {
      LOG.info("Doesn't have access to getDefaultBlockSize on FileSystems", e);
      m = null; // could happen on setAccessible()
    }
    if (m == null) {
      return fs.getDefaultBlockSize();
    } else {
      try {
        Object ret = m.invoke(fs, path);
        return ((Long)ret).longValue();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  }

  /*
   * Get the default replication.
   *
   * use reflection to search for getDefaultReplication(Path f)
   * if the method doesn't exist, fall back to using getDefaultReplication()
   *
   * @param fs filesystem object
   * @param f path of file
   * @return default replication for the path's filesystem
   * @throws IOException e
   */
  public static short getDefaultReplication(final FileSystem fs, final Path path) throws IOException {
    Method m = null;
    Class<? extends FileSystem> cls = fs.getClass();
    try {
      m = cls.getMethod("getDefaultReplication", new Class<?>[] { Path.class });
    } catch (NoSuchMethodException e) {
      LOG.info("FileSystem doesn't support getDefaultReplication");
    } catch (SecurityException e) {
      LOG.info("Doesn't have access to getDefaultReplication on FileSystems", e);
      m = null; // could happen on setAccessible()
    }
    if (m == null) {
      return fs.getDefaultReplication();
    } else {
      try {
        Object ret = m.invoke(fs, path);
        return ((Number)ret).shortValue();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  }

  /**
   * Returns the default buffer size to use during writes.
   *
   * The size of the buffer should probably be a multiple of hardware
   * page size (4096 on Intel x86), and it determines how much data is
   * buffered during read and write operations.
   *
   * @param fs filesystem object
   * @return default buffer size to use during writes
   */
  public static int getDefaultBufferSize(final FileSystem fs) {
    return fs.getConf().getInt("io.file.buffer.size", 4096);
  }

  /**
   * Sets version of file system
   *
   * @param fs filesystem object
   * @param rootdir hbase root directory
   * @param version version to set
   * @param wait time to wait for retry
   * @param retries number of times to retry before throwing an IOException
   * @throws IOException e
   */
  public static void setVersion(FileSystem fs, Path rootdir, String version,
      int wait, int retries) throws IOException {
    Path versionFile = new Path(rootdir, HConstants.VERSION_FILE_NAME);
    Path tmpFile = new Path(new Path(rootdir, HConstants.HBASE_TEMP_DIRECTORY), HConstants.VERSION_FILE_NAME);
    while (true) {
      try {
        FSDataOutputStream s = fs.create(tmpFile);
        s.writeUTF(version);
        s.close();
        if (!fs.rename(tmpFile, versionFile)) {
          throw new IOException("Unable to move temp version file to " + versionFile);
        }
        LOG.debug("Created version file at " + rootdir.toString() +
            " set its version at:" + version);
        return;
      } catch (IOException e) {
        if (retries > 0) {
          LOG.warn("Unable to create version file at " + rootdir.toString() +
              ", retrying: " + e.getMessage());
          fs.delete(versionFile, false);
          try {
            if (wait > 0) {
              Thread.sleep(wait);
            }
          } catch (InterruptedException ex) {
            // ignore
          }
          retries--;
        } else {
          throw e;
        }
      }
    }
  }

  /**
   * Checks that a cluster ID file exists in the HBase root directory
   * @param fs the root directory FileSystem
   * @param rootdir the HBase root directory in HDFS
   * @param wait how long to wait between retries
   * @return <code>true</code> if the file exists, otherwise <code>false</code>
   * @throws IOException if checking the FileSystem fails
   */
  public static boolean checkClusterIdExists(FileSystem fs, Path rootdir,
      int wait) throws IOException {
    while (true) {
      try {
        Path filePath = new Path(rootdir, HConstants.CLUSTER_ID_FILE_NAME);
        return fs.exists(filePath);
      } catch (IOException ioe) {
        if (wait > 0) {
          LOG.warn("Unable to check cluster ID file in " + rootdir.toString() +
              ", retrying in "+wait+"msec: "+StringUtils.stringifyException(ioe));
          try {
            Thread.sleep(wait);
          } catch (InterruptedException ie) {
            Thread.interrupted();
            break;
          }
        } else {
          throw ioe;
        }
      }
    }
    return false;
  }

  /**
   * Returns the value of the unique cluster ID stored for this HBase instance.
   * @param fs the root directory FileSystem
   * @param rootdir the path to the HBase root directory
   * @return the unique cluster identifier
   * @throws IOException if reading the cluster ID file fails
   */
  public static String getClusterId(FileSystem fs, Path rootdir)
      throws IOException {
    Path idPath = new Path(rootdir, HConstants.CLUSTER_ID_FILE_NAME);
    String clusterId = null;
    if (fs.exists(idPath)) {
      FSDataInputStream in = fs.open(idPath);
      try {
        clusterId = in.readUTF();
      } catch (EOFException eof) {
        LOG.warn("Cluster ID file "+idPath.toString()+" was empty");
      } finally{
        in.close();
      }
    } else {
      LOG.warn("Cluster ID file does not exist at " + idPath.toString());
    }
    return clusterId;
  }

  /**
   * Writes a new unique identifier for this cluster to the "hbase.id" file
   * in the HBase root directory
   * @param fs the root directory FileSystem
   * @param rootdir the path to the HBase root directory
   * @param clusterId the unique identifier to store
   * @param wait how long (in milliseconds) to wait between retries
   * @throws IOException if writing to the FileSystem fails and no wait value
   */
  public static void setClusterId(FileSystem fs, Path rootdir, String clusterId,
      int wait) throws IOException {
    Path idFfile = new Path(rootdir, HConstants.CLUSTER_ID_FILE_NAME);
    Path tmpFile = new Path(new Path(rootdir, HConstants.HBASE_TEMP_DIRECTORY), HConstants.CLUSTER_ID_FILE_NAME);
    while (true) {
      try {
        FSDataOutputStream s = fs.create(tmpFile);
        s.writeUTF(clusterId);
        s.close();
        if (!fs.rename(tmpFile, idFfile)) {
          throw new IOException("Unable to move temp version file to " + idFfile);
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Created cluster ID file at " + idFfile.toString() +
              " with ID: " + clusterId);
        }
        return;
      } catch (IOException ioe) {
        if (wait > 0) {
          LOG.warn("Unable to create cluster ID file in " + rootdir.toString() +
              ", retrying in "+wait+"msec: "+StringUtils.stringifyException(ioe));
          try {
            Thread.sleep(wait);
          } catch (InterruptedException ie) {
            Thread.interrupted();
            break;
          }
        } else {
          throw ioe;
        }
      }
    }
  }

  /**
   * Verifies root directory path is a valid URI with a scheme
   *
   * @param root root directory path
   * @return Passed <code>root</code> argument.
   * @throws IOException if not a valid URI with a scheme
   */
  public static Path validateRootPath(Path root) throws IOException {
    try {
      URI rootURI = new URI(root.toString());
      String scheme = rootURI.getScheme();
      if (scheme == null) {
        throw new IOException("Root directory does not have a scheme");
      }
      return root;
    } catch (URISyntaxException e) {
      IOException io = new IOException("Root directory path is not a valid " +
        "URI -- check your " + HConstants.HBASE_DIR + " configuration");
      io.initCause(e);
      throw io;
    }
  }

  /**
   * If DFS, check safe mode and if so, wait until we clear it.
   * @param conf configuration
   * @param wait Sleep between retries
   * @throws IOException e
   */
  public static void waitOnSafeMode(final Configuration conf,
    final long wait)
  throws IOException {
    FileSystem fs = FileSystem.get(conf);
    if (!(fs instanceof DistributedFileSystem)) return;
    DistributedFileSystem dfs = (DistributedFileSystem)fs;
    // Make sure dfs is not in safe mode
    while (isInSafeMode(dfs)) {
      LOG.info("Waiting for dfs to exit safe mode...");
      try {
        Thread.sleep(wait);
      } catch (InterruptedException e) {
        //continue
      }
    }
  }

  /**
   * Return the 'path' component of a Path.  In Hadoop, Path is an URI.  This
   * method returns the 'path' component of a Path's URI: e.g. If a Path is
   * <code>hdfs://example.org:9000/hbase_trunk/TestTable/compaction.dir</code>,
   * this method returns <code>/hbase_trunk/TestTable/compaction.dir</code>.
   * This method is useful if you want to print out a Path without qualifying
   * Filesystem instance.
   * @param p Filesystem Path whose 'path' component we are to return.
   * @return Path portion of the Filesystem
   */
  public static String getPath(Path p) {
    return p.toUri().getPath();
  }

  /**
   * @param c configuration
   * @return Path to hbase root directory: i.e. <code>hbase.rootdir</code> from
   * configuration as a qualified Path.
   * @throws IOException e
   */
  public static Path getRootDir(final Configuration c) throws IOException {
    Path p = new Path(c.get(HConstants.HBASE_DIR));
    FileSystem fs = p.getFileSystem(c);
    return p.makeQualified(fs);
  }

  public static void setRootDir(final Configuration c, final Path root) throws IOException {
    c.set(HConstants.HBASE_DIR, root.toString());
  }

  public static void setFsDefault(final Configuration c, final Path root) throws IOException {
    c.set("fs.defaultFS", root.toString());    // for hadoop 0.21+
    c.set("fs.default.name", root.toString()); // for hadoop 0.20
  }

  /**
   * Checks if root region exists
   *
   * @param fs file system
   * @param rootdir root directory of HBase installation
   * @return true if exists
   * @throws IOException e
   */
  public static boolean rootRegionExists(FileSystem fs, Path rootdir)
  throws IOException {
    Path rootRegionDir =
      HRegion.getRegionDir(rootdir, HRegionInfo.ROOT_REGIONINFO);
    return fs.exists(rootRegionDir);
  }

  /**
   * Compute HDFS blocks distribution of a given file, or a portion of the file
   * @param fs file system
   * @param status file status of the file
   * @param start start position of the portion
   * @param length length of the portion
   * @return The HDFS blocks distribution
   */
  static public HDFSBlocksDistribution computeHDFSBlocksDistribution(
    final FileSystem fs, FileStatus status, long start, long length)
    throws IOException {
    HDFSBlocksDistribution blocksDistribution = new HDFSBlocksDistribution();
    BlockLocation [] blockLocations =
      fs.getFileBlockLocations(status, start, length);
    for(BlockLocation bl : blockLocations) {
      String [] hosts = bl.getHosts();
      long len = bl.getLength();
      blocksDistribution.addHostsAndBlockWeight(hosts, len);
    }

    return blocksDistribution;
  }



  /**
   * Runs through the hbase rootdir and checks all stores have only
   * one file in them -- that is, they've been major compacted.  Looks
   * at root and meta tables too.
   * @param fs filesystem
   * @param hbaseRootDir hbase root directory
   * @return True if this hbase install is major compacted.
   * @throws IOException e
   */
  public static boolean isMajorCompacted(final FileSystem fs,
      final Path hbaseRootDir)
  throws IOException {
    // Presumes any directory under hbase.rootdir is a table.
    FileStatus [] tableDirs = fs.listStatus(hbaseRootDir, new DirFilter(fs));
    for (FileStatus tableDir : tableDirs) {
      // Skip the .log directory.  All others should be tables.  Inside a table,
      // there are compaction.dir directories to skip.  Otherwise, all else
      // should be regions.  Then in each region, should only be family
      // directories.  Under each of these, should be one file only.
      Path d = tableDir.getPath();
      if (d.getName().equals(HConstants.HREGION_LOGDIR_NAME)) {
        continue;
      }
      FileStatus[] regionDirs = fs.listStatus(d, new DirFilter(fs));
      for (FileStatus regionDir : regionDirs) {
        Path dd = regionDir.getPath();
        if (dd.getName().equals(HConstants.HREGION_COMPACTIONDIR_NAME)) {
          continue;
        }
        // Else its a region name.  Now look in region for families.
        FileStatus[] familyDirs = fs.listStatus(dd, new DirFilter(fs));
        for (FileStatus familyDir : familyDirs) {
          Path family = familyDir.getPath();
          // Now in family make sure only one file.
          FileStatus[] familyStatus = fs.listStatus(family);
          if (familyStatus.length > 1) {
            LOG.debug(family.toString() + " has " + familyStatus.length +
                " files.");
            return false;
          }
        }
      }
    }
    return true;
  }

  // TODO move this method OUT of FSUtils. No dependencies to HMaster
  /**
   * Returns the total overall fragmentation percentage. Includes .META. and
   * -ROOT- as well.
   *
   * @param master  The master defining the HBase root and file system.
   * @return A map for each table and its percentage.
   * @throws IOException When scanning the directory fails.
   */
  public static int getTotalTableFragmentation(final HMaster master)
  throws IOException {
    Map<String, Integer> map = getTableFragmentation(master);
    return map != null && map.size() > 0 ? map.get("-TOTAL-") : -1;
  }

  /**
   * Runs through the HBase rootdir and checks how many stores for each table
   * have more than one file in them. Checks -ROOT- and .META. too. The total
   * percentage across all tables is stored under the special key "-TOTAL-".
   *
   * @param master  The master defining the HBase root and file system.
   * @return A map for each table and its percentage.
   * @throws IOException When scanning the directory fails.
   */
  public static Map<String, Integer> getTableFragmentation(
    final HMaster master)
  throws IOException {
    Path path = getRootDir(master.getConfiguration());
    // since HMaster.getFileSystem() is package private
    FileSystem fs = path.getFileSystem(master.getConfiguration());
    return getTableFragmentation(fs, path);
  }

  /**
   * Runs through the HBase rootdir and checks how many stores for each table
   * have more than one file in them. Checks -ROOT- and .META. too. The total
   * percentage across all tables is stored under the special key "-TOTAL-".
   *
   * @param fs  The file system to use.
   * @param hbaseRootDir  The root directory to scan.
   * @return A map for each table and its percentage.
   * @throws IOException When scanning the directory fails.
   */
  public static Map<String, Integer> getTableFragmentation(
    final FileSystem fs, final Path hbaseRootDir)
  throws IOException {
    Map<String, Integer> frags = new HashMap<String, Integer>();
    int cfCountTotal = 0;
    int cfFragTotal = 0;
    DirFilter df = new DirFilter(fs);
    // presumes any directory under hbase.rootdir is a table
    FileStatus [] tableDirs = fs.listStatus(hbaseRootDir, df);
    for (FileStatus tableDir : tableDirs) {
      // Skip the .log directory.  All others should be tables.  Inside a table,
      // there are compaction.dir directories to skip.  Otherwise, all else
      // should be regions.  Then in each region, should only be family
      // directories.  Under each of these, should be one file only.
      Path d = tableDir.getPath();
      if (d.getName().equals(HConstants.HREGION_LOGDIR_NAME)) {
        continue;
      }
      int cfCount = 0;
      int cfFrag = 0;
      FileStatus[] regionDirs = fs.listStatus(d, df);
      for (FileStatus regionDir : regionDirs) {
        Path dd = regionDir.getPath();
        if (dd.getName().equals(HConstants.HREGION_COMPACTIONDIR_NAME)) {
          continue;
        }
        // else its a region name, now look in region for families
        FileStatus[] familyDirs = fs.listStatus(dd, df);
        for (FileStatus familyDir : familyDirs) {
          cfCount++;
          cfCountTotal++;
          Path family = familyDir.getPath();
          // now in family make sure only one file
          FileStatus[] familyStatus = fs.listStatus(family);
          if (familyStatus.length > 1) {
            cfFrag++;
            cfFragTotal++;
          }
        }
      }
      // compute percentage per table and store in result list
      frags.put(d.getName(), Math.round((float) cfFrag / cfCount * 100));
    }
    // set overall percentage for all tables
    frags.put("-TOTAL-", Math.round((float) cfFragTotal / cfCountTotal * 100));
    return frags;
  }

  /**
   * Expects to find -ROOT- directory.
   * @param fs filesystem
   * @param hbaseRootDir hbase root directory
   * @return True if this a pre020 layout.
   * @throws IOException e
   */
  public static boolean isPre020FileLayout(final FileSystem fs,
    final Path hbaseRootDir)
  throws IOException {
    Path mapfiles = new Path(new Path(new Path(new Path(hbaseRootDir, "-ROOT-"),
      "70236052"), "info"), "mapfiles");
    return fs.exists(mapfiles);
  }

  /**
   * Runs through the hbase rootdir and checks all stores have only
   * one file in them -- that is, they've been major compacted.  Looks
   * at root and meta tables too.  This version differs from
   * {@link #isMajorCompacted(FileSystem, Path)} in that it expects a
   * pre-0.20.0 hbase layout on the filesystem.  Used migrating.
   * @param fs filesystem
   * @param hbaseRootDir hbase root directory
   * @return True if this hbase install is major compacted.
   * @throws IOException e
   */
  public static boolean isMajorCompactedPre020(final FileSystem fs,
      final Path hbaseRootDir)
  throws IOException {
    // Presumes any directory under hbase.rootdir is a table.
    FileStatus [] tableDirs = fs.listStatus(hbaseRootDir, new DirFilter(fs));
    for (FileStatus tableDir : tableDirs) {
      // Inside a table, there are compaction.dir directories to skip.
      // Otherwise, all else should be regions.  Then in each region, should
      // only be family directories.  Under each of these, should be a mapfile
      // and info directory and in these only one file.
      Path d = tableDir.getPath();
      if (d.getName().equals(HConstants.HREGION_LOGDIR_NAME)) {
        continue;
      }
      FileStatus[] regionDirs = fs.listStatus(d, new DirFilter(fs));
      for (FileStatus regionDir : regionDirs) {
        Path dd = regionDir.getPath();
        if (dd.getName().equals(HConstants.HREGION_COMPACTIONDIR_NAME)) {
          continue;
        }
        // Else its a region name.  Now look in region for families.
        FileStatus[] familyDirs = fs.listStatus(dd, new DirFilter(fs));
        for (FileStatus familyDir : familyDirs) {
          Path family = familyDir.getPath();
          FileStatus[] infoAndMapfile = fs.listStatus(family);
          // Assert that only info and mapfile in family dir.
          if (infoAndMapfile.length != 0 && infoAndMapfile.length != 2) {
            LOG.debug(family.toString() +
                " has more than just info and mapfile: " + infoAndMapfile.length);
            return false;
          }
          // Make sure directory named info or mapfile.
          for (int ll = 0; ll < 2; ll++) {
            if (infoAndMapfile[ll].getPath().getName().equals("info") ||
                infoAndMapfile[ll].getPath().getName().equals("mapfiles"))
              continue;
            LOG.debug("Unexpected directory name: " +
                infoAndMapfile[ll].getPath());
            return false;
          }
          // Now in family, there are 'mapfile' and 'info' subdirs.  Just
          // look in the 'mapfile' subdir.
          FileStatus[] familyStatus =
              fs.listStatus(new Path(family, "mapfiles"));
          if (familyStatus.length > 1) {
            LOG.debug(family.toString() + " has " + familyStatus.length +
                " files.");
            return false;
          }
        }
      }
    }
    return true;
  }

  /**
   * A {@link PathFilter} that returns only regular files.
   */
  static class FileFilter implements PathFilter {
    private final FileSystem fs;

    public FileFilter(final FileSystem fs) {
      this.fs = fs;
    }

    @Override
    public boolean accept(Path p) {
      try {
        return fs.isFile(p);
      } catch (IOException e) {
        LOG.debug("unable to verify if path=" + p + " is a regular file", e);
        return false;
      }
    }
  }

  /**
   * A {@link PathFilter} that returns directories.
   */
  public static class DirFilter implements PathFilter {
    private final FileSystem fs;

    public DirFilter(final FileSystem fs) {
      this.fs = fs;
    }

    @Override
    public boolean accept(Path p) {
      boolean isValid = false;
      try {
        if (HConstants.HBASE_NON_USER_TABLE_DIRS.contains(p)) {
          isValid = false;
        } else {
          isValid = this.fs.getFileStatus(p).isDir();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
      return isValid;
    }
  }

  /**
   * Heuristic to determine whether is safe or not to open a file for append
   * Looks both for dfs.support.append and use reflection to search
   * for SequenceFile.Writer.syncFs() or FSDataOutputStream.hflush()
   * @param conf
   * @return True if append support
   */
  public static boolean isAppendSupported(final Configuration conf) {
    boolean append = conf.getBoolean("dfs.support.append", false);
    if (append) {
      try {
        // TODO: The implementation that comes back when we do a createWriter
        // may not be using SequenceFile so the below is not a definitive test.
        // Will do for now (hdfs-200).
        SequenceFile.Writer.class.getMethod("syncFs", new Class<?> []{});
        append = true;
      } catch (SecurityException e) {
      } catch (NoSuchMethodException e) {
        append = false;
      }
    }
    if (!append) {
      // Look for the 0.21, 0.22, new-style append evidence.
      try {
        FSDataOutputStream.class.getMethod("hflush", new Class<?> []{});
        append = true;
      } catch (NoSuchMethodException e) {
        append = false;
      }
    }
    return append;
  }

  /**
   * @param conf
   * @return True if this filesystem whose scheme is 'hdfs'.
   * @throws IOException
   */
  public static boolean isHDFS(final Configuration conf) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    String scheme = fs.getUri().getScheme();
    return scheme.equalsIgnoreCase("hdfs");
  }

  /**
   * Recover file lease. Used when a file might be suspect
   * to be had been left open by another process.
   * @param fs FileSystem handle
   * @param p Path of file to recover lease
   * @param conf Configuration handle
   * @throws IOException
   */
  public abstract void recoverFileLease(final FileSystem fs, final Path p,
      Configuration conf) throws IOException;

  /**
   * @param fs
   * @param rootdir
   * @return All the table directories under <code>rootdir</code>. Ignore non table hbase folders such as
   * .logs, .oldlogs, .corrupt, .META., and -ROOT- folders.
   * @throws IOException
   */
  public static List<Path> getTableDirs(final FileSystem fs, final Path rootdir)
  throws IOException {
    // presumes any directory under hbase.rootdir is a table
    FileStatus [] dirs = fs.listStatus(rootdir, new DirFilter(fs));
    List<Path> tabledirs = new ArrayList<Path>(dirs.length);
    for (FileStatus dir: dirs) {
      Path p = dir.getPath();
      String tableName = p.getName();
      if (!HConstants.HBASE_NON_USER_TABLE_DIRS.contains(tableName)) {
        tabledirs.add(p);
      }
    }
    return tabledirs;
  }

  public static Path getTablePath(Path rootdir, byte [] tableName) {
    return getTablePath(rootdir, Bytes.toString(tableName));
  }

  public static Path getTablePath(Path rootdir, final String tableName) {
    return new Path(rootdir, tableName);
  }

  /**
   * Filter for all dirs that don't start with '.'
   */
  public static class RegionDirFilter implements PathFilter {
    // This pattern will accept 0.90+ style hex region dirs and older numeric region dir names.
    final public static Pattern regionDirPattern = Pattern.compile("^[0-9a-f]*$");
    final FileSystem fs;

    public RegionDirFilter(FileSystem fs) {
      this.fs = fs;
    }

    @Override
    public boolean accept(Path rd) {
      if (!regionDirPattern.matcher(rd.getName()).matches()) {
        return false;
      }

      try {
        return fs.getFileStatus(rd).isDir();
      } catch (IOException ioe) {
        // Maybe the file was moved or the fs was disconnected.
        LOG.warn("Skipping file " + rd +" due to IOException", ioe);
        return false;
      }
    }
  }

  /**
   * Given a particular table dir, return all the regiondirs inside it, excluding files such as
   * .tableinfo
   * @param fs A file system for the Path
   * @param tableDir Path to a specific table directory <hbase.rootdir>/<tabledir>
   * @return List of paths to valid region directories in table dir.
   * @throws IOException
   */
  public static List<Path> getRegionDirs(final FileSystem fs, final Path tableDir) throws IOException {
    // assumes we are in a table dir.
    FileStatus[] rds = fs.listStatus(tableDir, new RegionDirFilter(fs));
    List<Path> regionDirs = new ArrayList<Path>(rds.length);
    for (FileStatus rdfs: rds) {
      Path rdPath = rdfs.getPath();
      regionDirs.add(rdPath);
    }
    return regionDirs;
  }

  /**
   * Filter for all dirs that are legal column family names.  This is generally used for colfam
   * dirs <hbase.rootdir>/<tabledir>/<regiondir>/<colfamdir>.
   */
  public static class FamilyDirFilter implements PathFilter {
    final FileSystem fs;

    public FamilyDirFilter(FileSystem fs) {
      this.fs = fs;
    }

    @Override
    public boolean accept(Path rd) {
      try {
        // throws IAE if invalid
        HColumnDescriptor.isLegalFamilyName(Bytes.toBytes(rd.getName()));
      } catch (IllegalArgumentException iae) {
        // path name is an invalid family name and thus is excluded.
        return false;
      }

      try {
        return fs.getFileStatus(rd).isDir();
      } catch (IOException ioe) {
        // Maybe the file was moved or the fs was disconnected.
        LOG.warn("Skipping file " + rd +" due to IOException", ioe);
        return false;
      }
    }
  }

  /**
   * Given a particular region dir, return all the familydirs inside it
   *
   * @param fs A file system for the Path
   * @param regionDir Path to a specific region directory
   * @return List of paths to valid family directories in region dir.
   * @throws IOException
   */
  public static List<Path> getFamilyDirs(final FileSystem fs, final Path regionDir) throws IOException {
    // assumes we are in a region dir.
    FileStatus[] fds = fs.listStatus(regionDir, new FamilyDirFilter(fs));
    List<Path> familyDirs = new ArrayList<Path>(fds.length);
    for (FileStatus fdfs: fds) {
      Path fdPath = fdfs.getPath();
      familyDirs.add(fdPath);
    }
    return familyDirs;
  }

  /**
   * Filter for HFiles that excludes reference files.
   */
  public static class HFileFilter implements PathFilter {
    // This pattern will accept 0.90+ style hex hfies files but reject reference files
    final public static Pattern hfilePattern = Pattern.compile("^([0-9a-f]+)$");

    final FileSystem fs;

    public HFileFilter(FileSystem fs) {
      this.fs = fs;
    }

    @Override
    public boolean accept(Path rd) {
      if (!hfilePattern.matcher(rd.getName()).matches()) {
        return false;
      }

      try {
        // only files
        return !fs.getFileStatus(rd).isDir();
      } catch (IOException ioe) {
        // Maybe the file was moved or the fs was disconnected.
        LOG.warn("Skipping file " + rd +" due to IOException", ioe);
        return false;
      }
    }
  }

  /**
   * @param conf
   * @return Returns the filesystem of the hbase rootdir.
   * @throws IOException
   */
  public static FileSystem getCurrentFileSystem(Configuration conf)
  throws IOException {
    return getRootDir(conf).getFileSystem(conf);
  }

  /**
   * Runs through the HBase rootdir and creates a reverse lookup map for
   * table StoreFile names to the full Path.
   * <br>
   * Example...<br>
   * Key = 3944417774205889744  <br>
   * Value = hdfs://localhost:51169/user/userid/-ROOT-/70236052/info/3944417774205889744
   *
   * @param fs  The file system to use.
   * @param hbaseRootDir  The root directory to scan.
   * @return Map keyed by StoreFile name with a value of the full Path.
   * @throws IOException When scanning the directory fails.
   */
  public static Map<String, Path> getTableStoreFilePathMap(
    final FileSystem fs, final Path hbaseRootDir)
  throws IOException {
    Map<String, Path> map = new HashMap<String, Path>();
    
    // if this method looks similar to 'getTableFragmentation' that is because 
    // it was borrowed from it.
    
    DirFilter df = new DirFilter(fs);
    // presumes any directory under hbase.rootdir is a table
    FileStatus [] tableDirs = fs.listStatus(hbaseRootDir, df);
    for (FileStatus tableDir : tableDirs) {
      // Skip the .log and other non-table directories.  All others should be tables.
      // Inside a table, there are compaction.dir directories to skip.  Otherwise, all else
      // should be regions. 
      Path d = tableDir.getPath();
      if (HConstants.HBASE_NON_TABLE_DIRS.contains(d.getName())) {
        continue;
      }
      FileStatus[] regionDirs = fs.listStatus(d, df);
      for (FileStatus regionDir : regionDirs) {
        Path dd = regionDir.getPath();
        if (dd.getName().equals(HConstants.HREGION_COMPACTIONDIR_NAME)) {
          continue;
        }
        // else its a region name, now look in region for families
        FileStatus[] familyDirs = fs.listStatus(dd, df);
        for (FileStatus familyDir : familyDirs) {
          Path family = familyDir.getPath();
          // now in family, iterate over the StoreFiles and
          // put in map
          FileStatus[] familyStatus = fs.listStatus(family);
          for (FileStatus sfStatus : familyStatus) {
            Path sf = sfStatus.getPath();
            map.put( sf.getName(), sf);
          }

        }
      }
    }
      return map;
  }

  /**
   * Calls fs.listStatus() and treats FileNotFoundException as non-fatal
   * This accommodates differences between hadoop versions
   *
   * @param fs file system
   * @param dir directory
   * @param filter path filter
   * @return null if dir is empty or doesn't exist, otherwise FileStatus array
   */
  public static FileStatus [] listStatus(final FileSystem fs,
      final Path dir, final PathFilter filter) throws IOException {
    FileStatus [] status = null;
    try {
      status = filter == null ? fs.listStatus(dir) : fs.listStatus(dir, filter);
    } catch (FileNotFoundException fnfe) {
      // if directory doesn't exist, return null
      LOG.debug(dir + " doesn't exist");
    }
    if (status == null || status.length < 1) return null;
    return status;
  }

  /**
   * Calls fs.listStatus() and treats FileNotFoundException as non-fatal
   * This would accommodates differences between hadoop versions
   *
   * @param fs file system
   * @param dir directory
   * @return null if dir is empty or doesn't exist, otherwise FileStatus array
   */
  public static FileStatus[] listStatus(final FileSystem fs, final Path dir) throws IOException {
    return listStatus(fs, dir, null);
  }

  /**
   * Calls fs.delete() and returns the value returned by the fs.delete()
   *
   * @param fs
   * @param path
   * @param recursive
   * @return
   * @throws IOException
   */
  public static boolean delete(final FileSystem fs, final Path path, final boolean recursive)
      throws IOException {
    return fs.delete(path, recursive);
  }

  /**
   * Throw an exception if an action is not permitted by a user on a file.
   * 
   * @param user
   *          the user
   * @param file
   *          the file
   * @param action
   *          the action
   */
  public static void checkAccess(User user, FileStatus file,
      FsAction action) throws AccessControlException {
    // See HBASE-7814. UserGroupInformation from hadoop 0.20.x may not support getShortName().
    String username = user.getShortName();
    if (username.equals(file.getOwner())) {
      if (file.getPermission().getUserAction().implies(action)) {
        return;
      }
    } else if (contains(user.getGroupNames(), file.getGroup())) {
      if (file.getPermission().getGroupAction().implies(action)) {
        return;
      }
    } else if (file.getPermission().getOtherAction().implies(action)) {
      return;
    }
    throw new AccessControlException("Permission denied:" + " action=" + action
        + " path=" + file.getPath() + " user=" + username);
  }

  private static boolean contains(String[] groups, String user) {
    for (String group : groups) {
      if (group.equals(user)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Calls fs.exists(). Checks if the specified path exists
   *
   * @param fs
   * @param path
   * @return
   * @throws IOException
   */
  public static boolean isExists(final FileSystem fs, final Path path) throws IOException {
    return fs.exists(path);
  }

  /**
   * Log the current state of the filesystem from a certain root directory
   * @param fs filesystem to investigate
   * @param root root file/directory to start logging from
   * @param LOG log to output information
   * @throws IOException if an unexpected exception occurs
   */
  public static void logFileSystemState(final FileSystem fs, final Path root, Log LOG)
      throws IOException {
    LOG.debug("Current file system:");
    logFSTree(LOG, fs, root, "|-");
  }

  /**
   * Recursive helper to log the state of the FS
   * @see #logFileSystemState(FileSystem, Path, Log)
   */
  private static void logFSTree(Log LOG, final FileSystem fs, final Path root, String prefix)
      throws IOException {
    FileStatus[] files = FSUtils.listStatus(fs, root, null);
    if (files == null) return;

    for (FileStatus file : files) {
      if (file.isDir()) {
        LOG.debug(prefix + file.getPath().getName() + "/");
        logFSTree(LOG, fs, file.getPath(), prefix + "---");
      } else {
        LOG.debug(prefix + file.getPath().getName());
      }
    }
  }
}
