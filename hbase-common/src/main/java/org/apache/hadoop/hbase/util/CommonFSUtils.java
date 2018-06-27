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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

/**
 * Utility methods for interacting with the underlying file system.
 * <p/>
 * Note that {@link #setStoragePolicy(FileSystem, Path, String)} is tested in TestFSUtils and
 * pre-commit will run the hbase-server tests if there's code change in this class. See
 * <a href="https://issues.apache.org/jira/browse/HBASE-20838">HBASE-20838</a> for more details.
 */
@InterfaceAudience.Private
public abstract class CommonFSUtils {
  private static final Logger LOG = LoggerFactory.getLogger(CommonFSUtils.class);

  /** Parameter name for HBase WAL directory */
  public static final String HBASE_WAL_DIR = "hbase.wal.dir";

  /** Parameter to disable stream capability enforcement checks */
  public static final String UNSAFE_STREAM_CAPABILITY_ENFORCE = "hbase.unsafe.stream.capability.enforce";

  /** Full access permissions (starting point for a umask) */
  public static final String FULL_RWX_PERMISSIONS = "777";

  protected CommonFSUtils() {
    super();
  }

  /**
   * Compare of path component. Does not consider schema; i.e. if schemas
   * different but <code>path</code> starts with <code>rootPath</code>,
   * then the function returns true
   * @param rootPath value to check for
   * @param path subject to check
   * @return True if <code>path</code> starts with <code>rootPath</code>
   */
  public static boolean isStartingWithPath(final Path rootPath, final String path) {
    String uriRootPath = rootPath.toUri().getPath();
    String tailUriPath = (new Path(path)).toUri().getPath();
    return tailUriPath.startsWith(uriRootPath);
  }

  /**
   * Compare path component of the Path URI; e.g. if hdfs://a/b/c and /a/b/c, it will compare the
   * '/a/b/c' part. Does not consider schema; i.e. if schemas different but path or subpath matches,
   * the two will equate.
   * @param pathToSearch Path we will be trying to match against.
   * @param pathTail what to match
   * @return True if <code>pathTail</code> is tail on the path of <code>pathToSearch</code>
   */
  public static boolean isMatchingTail(final Path pathToSearch, String pathTail) {
    return isMatchingTail(pathToSearch, new Path(pathTail));
  }

  /**
   * Compare path component of the Path URI; e.g. if hdfs://a/b/c and /a/b/c, it will compare the
   * '/a/b/c' part. If you passed in 'hdfs://a/b/c and b/c, it would return true.  Does not consider
   * schema; i.e. if schemas different but path or subpath matches, the two will equate.
   * @param pathToSearch Path we will be trying to match agains against
   * @param pathTail what to match
   * @return True if <code>pathTail</code> is tail on the path of <code>pathToSearch</code>
   */
  public static boolean isMatchingTail(final Path pathToSearch, final Path pathTail) {
    if (pathToSearch.depth() != pathTail.depth()) {
      return false;
    }
    Path tailPath = pathTail;
    String tailName;
    Path toSearch = pathToSearch;
    String toSearchName;
    boolean result = false;
    do {
      tailName = tailPath.getName();
      if (tailName == null || tailName.length() <= 0) {
        result = true;
        break;
      }
      toSearchName = toSearch.getName();
      if (toSearchName == null || toSearchName.length() <= 0) {
        break;
      }
      // Move up a parent on each path for next go around.  Path doesn't let us go off the end.
      tailPath = tailPath.getParent();
      toSearch = toSearch.getParent();
    } while(tailName.equals(toSearchName));
    return result;
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
      return fs.getDefaultBlockSize(path);
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
  public static short getDefaultReplication(final FileSystem fs, final Path path)
      throws IOException {
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
      return fs.getDefaultReplication(path);
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
   * Create the specified file on the filesystem. By default, this will:
   * <ol>
   * <li>apply the umask in the configuration (if it is enabled)</li>
   * <li>use the fs configured buffer size (or 4096 if not set)</li>
   * <li>use the default replication</li>
   * <li>use the default block size</li>
   * <li>not track progress</li>
   * </ol>
   *
   * @param fs {@link FileSystem} on which to write the file
   * @param path {@link Path} to the file to write
   * @param perm intial permissions
   * @param overwrite Whether or not the created file should be overwritten.
   * @return output stream to the created file
   * @throws IOException if the file cannot be created
   */
  public static FSDataOutputStream create(FileSystem fs, Path path,
      FsPermission perm, boolean overwrite) throws IOException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Creating file=" + path + " with permission=" + perm + ", overwrite=" + overwrite);
    }
    return fs.create(path, perm, overwrite, getDefaultBufferSize(fs),
        getDefaultReplication(fs, path), getDefaultBlockSize(fs, path), null);
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
        if (mask == null) {
          return FsPermission.getFileDefault();
        }
        // appy the umask
        FsPermission umask = new FsPermission(mask);
        return perm.applyUMask(umask);
      } catch (IllegalArgumentException e) {
        LOG.warn(
            "Incorrect umask attempted to be created: "
                + conf.get(permssionConfKey)
                + ", using default file permissions.", e);
        return FsPermission.getFileDefault();
      }
    }
    return FsPermission.getFileDefault();
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
   * Checks for the presence of the WAL log root path (using the provided conf object) in the given
   * path. If it exists, this method removes it and returns the String representation of remaining
   * relative path.
   * @param path must not be null
   * @param conf must not be null
   * @return String representation of the remaining relative path
   * @throws IOException from underlying filesystem
   */
  public static String removeWALRootPath(Path path, final Configuration conf) throws IOException {
    Path root = getWALRootDir(conf);
    String pathStr = path.toString();
    // check that the path is absolute... it has the root path in it.
    if (!pathStr.startsWith(root.toString())) {
      return pathStr;
    }
    // if not, return as it is.
    return pathStr.substring(root.toString().length() + 1);// remove the "/" too.
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
   * @return {@link Path} to hbase root directory from
   *     configuration as a qualified Path.
   * @throws IOException e
   */
  public static Path getRootDir(final Configuration c) throws IOException {
    Path p = new Path(c.get(HConstants.HBASE_DIR));
    FileSystem fs = p.getFileSystem(c);
    return p.makeQualified(fs.getUri(), fs.getWorkingDirectory());
  }

  public static void setRootDir(final Configuration c, final Path root) throws IOException {
    c.set(HConstants.HBASE_DIR, root.toString());
  }

  public static void setFsDefault(final Configuration c, final Path root) throws IOException {
    c.set("fs.defaultFS", root.toString());    // for hadoop 0.21+
  }

  public static FileSystem getRootDirFileSystem(final Configuration c) throws IOException {
    Path p = getRootDir(c);
    return p.getFileSystem(c);
  }

  /**
   * @param c configuration
   * @return {@link Path} to hbase log root directory: e.g. {@value HBASE_WAL_DIR} from
   *     configuration as a qualified Path. Defaults to HBase root dir.
   * @throws IOException e
   */
  public static Path getWALRootDir(final Configuration c) throws IOException {
    Path p = new Path(c.get(HBASE_WAL_DIR, c.get(HConstants.HBASE_DIR)));
    if (!isValidWALRootDir(p, c)) {
      return getRootDir(c);
    }
    FileSystem fs = p.getFileSystem(c);
    return p.makeQualified(fs.getUri(), fs.getWorkingDirectory());
  }

  @VisibleForTesting
  public static void setWALRootDir(final Configuration c, final Path root) throws IOException {
    c.set(HBASE_WAL_DIR, root.toString());
  }

  public static FileSystem getWALFileSystem(final Configuration c) throws IOException {
    Path p = getWALRootDir(c);
    FileSystem fs = p.getFileSystem(c);
    // hadoop-core does fs caching, so need to propogate this if set
    String enforceStreamCapability = c.get(UNSAFE_STREAM_CAPABILITY_ENFORCE);
    if (enforceStreamCapability != null) {
      fs.getConf().set(UNSAFE_STREAM_CAPABILITY_ENFORCE, enforceStreamCapability);
    }
    return fs;
  }

  private static boolean isValidWALRootDir(Path walDir, final Configuration c) throws IOException {
    Path rootDir = getRootDir(c);
    FileSystem fs = walDir.getFileSystem(c);
    Path qualifiedWalDir = walDir.makeQualified(fs.getUri(), fs.getWorkingDirectory());
    if (!qualifiedWalDir.equals(rootDir)) {
      if (qualifiedWalDir.toString().startsWith(rootDir.toString() + "/")) {
        throw new IllegalStateException("Illegal WAL directory specified. " +
            "WAL directories are not permitted to be under the root directory if set.");
      }
    }
    return true;
  }

  public static Path constructWALRegionDirFromRegionInfo(final Configuration conf,
      final TableName tableName, final String encodedRegionName)
      throws IOException {
    return new Path(getWALTableDir(conf, tableName),
        encodedRegionName);
  }

  public static Path getWALTableDir(final Configuration conf, final TableName tableName)
      throws IOException {
    return new Path(new Path(getWALRootDir(conf), tableName.getNamespaceAsString()),
        tableName.getQualifierAsString());
  }

  /**
   * Returns the {@link org.apache.hadoop.fs.Path} object representing the table directory under
   * path rootdir
   *
   * @param rootdir qualified path of HBase root directory
   * @param tableName name of table
   * @return {@link org.apache.hadoop.fs.Path} for table
   */
  public static Path getTableDir(Path rootdir, final TableName tableName) {
    return new Path(getNamespaceDir(rootdir, tableName.getNamespaceAsString()),
        tableName.getQualifierAsString());
  }

  /**
   * Returns the {@link org.apache.hadoop.hbase.TableName} object representing
   * the table directory under
   * path rootdir
   *
   * @param tablePath path of table
   * @return {@link org.apache.hadoop.fs.Path} for table
   */
  public static TableName getTableName(Path tablePath) {
    return TableName.valueOf(tablePath.getParent().getName(), tablePath.getName());
  }

  /**
   * Returns the {@link org.apache.hadoop.fs.Path} object representing
   * the namespace directory under path rootdir
   *
   * @param rootdir qualified path of HBase root directory
   * @param namespace namespace name
   * @return {@link org.apache.hadoop.fs.Path} for table
   */
  public static Path getNamespaceDir(Path rootdir, final String namespace) {
    return new Path(rootdir, new Path(HConstants.BASE_NAMESPACE_DIR,
        new Path(namespace)));
  }

  // this mapping means that under a federated FileSystem implementation, we'll
  // only log the first failure from any of the underlying FileSystems at WARN and all others
  // will be at DEBUG.
  private static final Map<FileSystem, Boolean> warningMap =
      new ConcurrentHashMap<FileSystem, Boolean>();

  /**
   * Sets storage policy for given path.
   * If the passed path is a directory, we'll set the storage policy for all files
   * created in the future in said directory. Note that this change in storage
   * policy takes place at the FileSystem level; it will persist beyond this RS's lifecycle.
   * If we're running on a version of FileSystem that doesn't support the given storage policy
   * (or storage policies at all), then we'll issue a log message and continue.
   *
   * See http://hadoop.apache.org/docs/r2.6.0/hadoop-project-dist/hadoop-hdfs/ArchivalStorage.html
   *
   * @param fs We only do anything it implements a setStoragePolicy method
   * @param path the Path whose storage policy is to be set
   * @param storagePolicy Policy to set on <code>path</code>; see hadoop 2.6+
   *   org.apache.hadoop.hdfs.protocol.HdfsConstants for possible list e.g
   *   'COLD', 'WARM', 'HOT', 'ONE_SSD', 'ALL_SSD', 'LAZY_PERSIST'.
   */
  public static void setStoragePolicy(final FileSystem fs, final Path path,
      final String storagePolicy) {
    try {
      setStoragePolicy(fs, path, storagePolicy, false);
    } catch (IOException e) {
      // should never arrive here
      LOG.warn("We have chosen not to throw exception but some unexpectedly thrown out", e);
    }
  }

  static void setStoragePolicy(final FileSystem fs, final Path path, final String storagePolicy,
      boolean throwException) throws IOException {
    if (storagePolicy == null) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("We were passed a null storagePolicy, exiting early.");
      }
      return;
    }
    String trimmedStoragePolicy = storagePolicy.trim();
    if (trimmedStoragePolicy.isEmpty()) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("We were passed an empty storagePolicy, exiting early.");
      }
      return;
    } else {
      trimmedStoragePolicy = trimmedStoragePolicy.toUpperCase(Locale.ROOT);
    }
    if (trimmedStoragePolicy.equals(HConstants.DEFER_TO_HDFS_STORAGE_POLICY)) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("We were passed the defer-to-hdfs policy {}, exiting early.",
          trimmedStoragePolicy);
      }
      return;
    }
    try {
      invokeSetStoragePolicy(fs, path, trimmedStoragePolicy);
    } catch (IOException e) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Failed to invoke set storage policy API on FS", e);
      }
      if (throwException) {
        throw e;
      }
    }
  }

  /*
   * All args have been checked and are good. Run the setStoragePolicy invocation.
   */
  private static void invokeSetStoragePolicy(final FileSystem fs, final Path path,
      final String storagePolicy) throws IOException {
    Method m = null;
    Exception toThrow = null;
    try {
      m = fs.getClass().getDeclaredMethod("setStoragePolicy",
        new Class<?>[] { Path.class, String.class });
      m.setAccessible(true);
    } catch (NoSuchMethodException e) {
      toThrow = e;
      final String msg = "FileSystem doesn't support setStoragePolicy; HDFS-6584, HDFS-9345 " +
          "not available. This is normal and expected on earlier Hadoop versions.";
      if (!warningMap.containsKey(fs)) {
        warningMap.put(fs, true);
        LOG.warn(msg, e);
      } else if (LOG.isDebugEnabled()) {
        LOG.debug(msg, e);
      }
      m = null;
    } catch (SecurityException e) {
      toThrow = e;
      final String msg = "No access to setStoragePolicy on FileSystem from the SecurityManager; " +
          "HDFS-6584, HDFS-9345 not available. This is unusual and probably warrants an email " +
          "to the user@hbase mailing list. Please be sure to include a link to your configs, and " +
          "logs that include this message and period of time before it. Logs around service " +
          "start up will probably be useful as well.";
      if (!warningMap.containsKey(fs)) {
        warningMap.put(fs, true);
        LOG.warn(msg, e);
      } else if (LOG.isDebugEnabled()) {
        LOG.debug(msg, e);
      }
      m = null; // could happen on setAccessible() or getDeclaredMethod()
    }
    if (m != null) {
      try {
        m.invoke(fs, path, storagePolicy);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Set storagePolicy=" + storagePolicy + " for path=" + path);
        }
      } catch (Exception e) {
        toThrow = e;
        // This swallows FNFE, should we be throwing it? seems more likely to indicate dev
        // misuse than a runtime problem with HDFS.
        if (!warningMap.containsKey(fs)) {
          warningMap.put(fs, true);
          LOG.warn("Unable to set storagePolicy=" + storagePolicy + " for path=" + path + ". " +
              "DEBUG log level might have more details.", e);
        } else if (LOG.isDebugEnabled()) {
          LOG.debug("Unable to set storagePolicy=" + storagePolicy + " for path=" + path, e);
        }
        // check for lack of HDFS-7228
        if (e instanceof InvocationTargetException) {
          final Throwable exception = e.getCause();
          if (exception instanceof RemoteException &&
              HadoopIllegalArgumentException.class.getName().equals(
                ((RemoteException)exception).getClassName())) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Given storage policy, '" +storagePolicy +"', was rejected and probably " +
                "isn't a valid policy for the version of Hadoop you're running. I.e. if you're " +
                "trying to use SSD related policies then you're likely missing HDFS-7228. For " +
                "more information see the 'ArchivalStorage' docs for your Hadoop release.");
            }
          // Hadoop 2.8+, 3.0-a1+ added FileSystem.setStoragePolicy with a default implementation
          // that throws UnsupportedOperationException
          } else if (exception instanceof UnsupportedOperationException) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("The underlying FileSystem implementation doesn't support " +
                  "setStoragePolicy. This is probably intentional on their part, since HDFS-9345 " +
                  "appears to be present in your version of Hadoop. For more information check " +
                  "the Hadoop documentation on 'ArchivalStorage', the Hadoop FileSystem " +
                  "specification docs from HADOOP-11981, and/or related documentation from the " +
                  "provider of the underlying FileSystem (its name should appear in the " +
                  "stacktrace that accompanies this message). Note in particular that Hadoop's " +
                  "local filesystem implementation doesn't support storage policies.", exception);
            }
          }
        }
      }
    }
    if (toThrow != null) {
      throw new IOException(toThrow);
    }
  }

  /**
   * @param conf must not be null
   * @return True if this filesystem whose scheme is 'hdfs'.
   * @throws IOException from underlying FileSystem
   */
  public static boolean isHDFS(final Configuration conf) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    String scheme = fs.getUri().getScheme();
    return scheme.equalsIgnoreCase("hdfs");
  }

  /**
   * Checks if the given path is the one with 'recovered.edits' dir.
   * @param path must not be null
   * @return True if we recovered edits
   */
  public static boolean isRecoveredEdits(Path path) {
    return path.toString().contains(HConstants.RECOVERED_EDITS_DIR);
  }

  /**
   * @param conf must not be null
   * @return Returns the filesystem of the hbase rootdir.
   * @throws IOException from underlying FileSystem
   */
  public static FileSystem getCurrentFileSystem(Configuration conf)
  throws IOException {
    return getRootDir(conf).getFileSystem(conf);
  }

  /**
   * Calls fs.listStatus() and treats FileNotFoundException as non-fatal
   * This accommodates differences between hadoop versions, where hadoop 1
   * does not throw a FileNotFoundException, and return an empty FileStatus[]
   * while Hadoop 2 will throw FileNotFoundException.
   *
   * Where possible, prefer FSUtils#listStatusWithStatusFilter(FileSystem,
   * Path, FileStatusFilter) instead.
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
      if (LOG.isTraceEnabled()) {
        LOG.trace(dir + " doesn't exist");
      }
    }
    if (status == null || status.length < 1) {
      return null;
    }
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
   * Calls fs.listFiles() to get FileStatus and BlockLocations together for reducing rpc call
   *
   * @param fs file system
   * @param dir directory
   * @return LocatedFileStatus list
   */
  public static List<LocatedFileStatus> listLocatedStatus(final FileSystem fs,
      final Path dir) throws IOException {
    List<LocatedFileStatus> status = null;
    try {
      RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fs
          .listFiles(dir, false);
      while (locatedFileStatusRemoteIterator.hasNext()) {
        if (status == null) {
          status = Lists.newArrayList();
        }
        status.add(locatedFileStatusRemoteIterator.next());
      }
    } catch (FileNotFoundException fnfe) {
      // if directory doesn't exist, return null
      if (LOG.isTraceEnabled()) {
        LOG.trace(dir + " doesn't exist");
      }
    }
    return status;
  }

  /**
   * Calls fs.delete() and returns the value returned by the fs.delete()
   *
   * @param fs must not be null
   * @param path must not be null
   * @param recursive delete tree rooted at path
   * @return the value returned by the fs.delete()
   * @throws IOException from underlying FileSystem
   */
  public static boolean delete(final FileSystem fs, final Path path, final boolean recursive)
      throws IOException {
    return fs.delete(path, recursive);
  }

  /**
   * Calls fs.exists(). Checks if the specified path exists
   *
   * @param fs must not be null
   * @param path must not be null
   * @return the value returned by fs.exists()
   * @throws IOException from underlying FileSystem
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
  public static void logFileSystemState(final FileSystem fs, final Path root, Logger LOG)
      throws IOException {
    LOG.debug("File system contents for path " + root);
    logFSTree(LOG, fs, root, "|-");
  }

  /**
   * Recursive helper to log the state of the FS
   *
   * @see #logFileSystemState(FileSystem, Path, Logger)
   */
  private static void logFSTree(Logger LOG, final FileSystem fs, final Path root, String prefix)
      throws IOException {
    FileStatus[] files = listStatus(fs, root, null);
    if (files == null) {
      return;
    }

    for (FileStatus file : files) {
      if (file.isDirectory()) {
        LOG.debug(prefix + file.getPath().getName() + "/");
        logFSTree(LOG, fs, file.getPath(), prefix + "---");
      } else {
        LOG.debug(prefix + file.getPath().getName());
      }
    }
  }

  public static boolean renameAndSetModifyTime(final FileSystem fs, final Path src, final Path dest)
      throws IOException {
    // set the modify time for TimeToLive Cleaner
    fs.setTimes(src, EnvironmentEdgeManager.currentTime(), -1);
    return fs.rename(src, dest);
  }

  /**
   * Do our short circuit read setup.
   * Checks buffer size to use and whether to do checksumming in hbase or hdfs.
   * @param conf must not be null
   */
  public static void setupShortCircuitRead(final Configuration conf) {
    // Check that the user has not set the "dfs.client.read.shortcircuit.skip.checksum" property.
    boolean shortCircuitSkipChecksum =
      conf.getBoolean("dfs.client.read.shortcircuit.skip.checksum", false);
    boolean useHBaseChecksum = conf.getBoolean(HConstants.HBASE_CHECKSUM_VERIFICATION, true);
    if (shortCircuitSkipChecksum) {
      LOG.warn("Configuration \"dfs.client.read.shortcircuit.skip.checksum\" should not " +
        "be set to true." + (useHBaseChecksum ? " HBase checksum doesn't require " +
        "it, see https://issues.apache.org/jira/browse/HBASE-6868." : ""));
      assert !shortCircuitSkipChecksum; //this will fail if assertions are on
    }
    checkShortCircuitReadBufferSize(conf);
  }

  /**
   * Check if short circuit read buffer size is set and if not, set it to hbase value.
   * @param conf must not be null
   */
  public static void checkShortCircuitReadBufferSize(final Configuration conf) {
    final int defaultSize = HConstants.DEFAULT_BLOCKSIZE * 2;
    final int notSet = -1;
    // DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_BUFFER_SIZE_KEY is only defined in h2
    final String dfsKey = "dfs.client.read.shortcircuit.buffer.size";
    int size = conf.getInt(dfsKey, notSet);
    // If a size is set, return -- we will use it.
    if (size != notSet) {
      return;
    }
    // But short circuit buffer size is normally not set.  Put in place the hbase wanted size.
    int hbaseSize = conf.getInt("hbase." + dfsKey, defaultSize);
    conf.setIfUnset(dfsKey, Integer.toString(hbaseSize));
  }

  // Holder singleton idiom. JVM spec ensures this will be run at most once per Classloader, and
  // not until we attempt to reference it.
  private static class StreamCapabilities {
    public static final boolean PRESENT;
    public static final Class<?> CLASS;
    public static final Method METHOD;
    static {
      boolean tmp = false;
      Class<?> clazz = null;
      Method method = null;
      try {
        clazz = Class.forName("org.apache.hadoop.fs.StreamCapabilities");
        method = clazz.getMethod("hasCapability", String.class);
        tmp = true;
      } catch(ClassNotFoundException|NoSuchMethodException|SecurityException exception) {
        LOG.warn("Your Hadoop installation does not include the StreamCapabilities class from " +
                 "HDFS-11644, so we will skip checking if any FSDataOutputStreams actually " +
                 "support hflush/hsync. If you are running on top of HDFS this probably just " +
                 "means you have an older version and this can be ignored. If you are running on " +
                 "top of an alternate FileSystem implementation you should manually verify that " +
                 "hflush and hsync are implemented; otherwise you risk data loss and hard to " +
                 "diagnose errors when our assumptions are violated.");
        LOG.debug("The first request to check for StreamCapabilities came from this stacktrace.",
            exception);
      } finally {
        PRESENT = tmp;
        CLASS = clazz;
        METHOD = method;
      }
    }
  }

  /**
   * If our FileSystem version includes the StreamCapabilities class, check if
   * the given stream has a particular capability.
   * @param stream capabilities are per-stream instance, so check this one specifically. must not be
   *        null
   * @param capability what to look for, per Hadoop Common's FileSystem docs
   * @return true if there are no StreamCapabilities. false if there are, but this stream doesn't
   *         implement it. return result of asking the stream otherwise.
   */
  public static boolean hasCapability(FSDataOutputStream stream, String capability) {
    // be consistent whether or not StreamCapabilities is present
    if (stream == null) {
      throw new NullPointerException("stream parameter must not be null.");
    }
    // If o.a.h.fs.StreamCapabilities doesn't exist, assume everyone does everything
    // otherwise old versions of Hadoop will break.
    boolean result = true;
    if (StreamCapabilities.PRESENT) {
      // if StreamCapabilities is present, but the stream doesn't implement it
      // or we run into a problem invoking the method,
      // we treat that as equivalent to not declaring anything
      result = false;
      if (StreamCapabilities.CLASS.isAssignableFrom(stream.getClass())) {
        try {
          result = ((Boolean)StreamCapabilities.METHOD.invoke(stream, capability)).booleanValue();
        } catch (IllegalAccessException|IllegalArgumentException|InvocationTargetException
            exception) {
          LOG.warn("Your Hadoop installation's StreamCapabilities implementation doesn't match " +
              "our understanding of how it's supposed to work. Please file a JIRA and include " +
              "the following stack trace. In the mean time we're interpreting this behavior " +
              "difference as a lack of capability support, which will probably cause a failure.",
              exception);
        }
      }
    }
    return result;
  }

  /**
   * Helper exception for those cases where the place where we need to check a stream capability
   * is not where we have the needed context to explain the impact and mitigation for a lack.
   */
  public static class StreamLacksCapabilityException extends Exception {
    public StreamLacksCapabilityException(String message, Throwable cause) {
      super(message, cause);
    }
    public StreamLacksCapabilityException(String message) {
      super(message);
    }
  }

}
