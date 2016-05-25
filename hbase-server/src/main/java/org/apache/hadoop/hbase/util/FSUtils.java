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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.HadoopIllegalArgumentException;
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
import org.apache.hadoop.hbase.ClusterId;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.util.HBaseFsck.ErrorReporter;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.FSProtos;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

import com.google.common.primitives.Ints;

/**
 * Utility methods for interacting with the underlying file system.
 */
@InterfaceAudience.Private
public abstract class FSUtils {
  private static final Log LOG = LogFactory.getLog(FSUtils.class);

  /** Full access permissions (starting point for a umask) */
  public static final String FULL_RWX_PERMISSIONS = "777";
  private static final String THREAD_POOLSIZE = "hbase.client.localityCheck.threadPoolSize";
  private static final int DEFAULT_THREAD_POOLSIZE = 2;

  /** Set to true on Windows platforms */
  public static final boolean WINDOWS = System.getProperty("os.name").startsWith("Windows");

  protected FSUtils() {
    super();
  }

  /**
   * Sets storage policy for given path according to config setting.
   * If the passed path is a directory, we'll set the storage policy for all files
   * created in the future in said directory. Note that this change in storage
   * policy takes place at the HDFS level; it will persist beyond this RS's lifecycle.
   * If we're running on a version of HDFS that doesn't support the given storage policy
   * (or storage policies at all), then we'll issue a log message and continue.
   *
   * See http://hadoop.apache.org/docs/r2.6.0/hadoop-project-dist/hadoop-hdfs/ArchivalStorage.html
   *
   * @param fs We only do anything if an instance of DistributedFileSystem
   * @param conf used to look up storage policy with given key; not modified.
   * @param path the Path whose storage policy is to be set
   * @param policyKey e.g. HConstants.WAL_STORAGE_POLICY
   * @param defaultPolicy usually should be the policy NONE to delegate to HDFS
   */
  public static void setStoragePolicy(final FileSystem fs, final Configuration conf,
      final Path path, final String policyKey, final String defaultPolicy) {
    String storagePolicy = conf.get(policyKey, defaultPolicy).toUpperCase(Locale.ROOT);
    if (storagePolicy.equals(defaultPolicy)) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("default policy of " + defaultPolicy + " requested, exiting early.");
      }
      return;
    }
    if (fs instanceof DistributedFileSystem) {
      DistributedFileSystem dfs = (DistributedFileSystem)fs;
      // Once our minimum supported Hadoop version is 2.6.0 we can remove reflection.
      Class<? extends DistributedFileSystem> dfsClass = dfs.getClass();
      Method m = null;
      try {
        m = dfsClass.getDeclaredMethod("setStoragePolicy",
            new Class<?>[] { Path.class, String.class });
        m.setAccessible(true);
      } catch (NoSuchMethodException e) {
        LOG.info("FileSystem doesn't support"
            + " setStoragePolicy; --HDFS-6584 not available");
      } catch (SecurityException e) {
        LOG.info("Doesn't have access to setStoragePolicy on "
            + "FileSystems --HDFS-6584 not available", e);
        m = null; // could happen on setAccessible()
      }
      if (m != null) {
        try {
          m.invoke(dfs, path, storagePolicy);
          LOG.info("set " + storagePolicy + " for " + path);
        } catch (Exception e) {
          // check for lack of HDFS-7228
          boolean probablyBadPolicy = false;
          if (e instanceof InvocationTargetException) {
            final Throwable exception = e.getCause();
            if (exception instanceof RemoteException &&
                HadoopIllegalArgumentException.class.getName().equals(
                    ((RemoteException)exception).getClassName())) {
              LOG.warn("Given storage policy, '" + storagePolicy + "', was rejected and probably " +
                  "isn't a valid policy for the version of Hadoop you're running. I.e. if you're " +
                  "trying to use SSD related policies then you're likely missing HDFS-7228. For " +
                  "more information see the 'ArchivalStorage' docs for your Hadoop release.");
              LOG.debug("More information about the invalid storage policy.", exception);
              probablyBadPolicy = true;
            }
          }
          if (!probablyBadPolicy) {
            // This swallows FNFE, should we be throwing it? seems more likely to indicate dev
            // misuse than a runtime problem with HDFS.
            LOG.warn("Unable to set " + storagePolicy + " for " + path, e);
          }
        }
      }
    } else {
      LOG.info("FileSystem isn't an instance of DistributedFileSystem; presuming it doesn't " +
          "support setStoragePolicy.");
    }
  }

  /**
   * Compare of path component. Does not consider schema; i.e. if schemas
   * different but <code>path</code> starts with <code>rootPath</code>,
   * then the function returns true
   * @param rootPath
   * @param path
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
   * @param pathToSearch Path we will be trying to match.
   * @param pathTail
   * @return True if <code>pathTail</code> is tail on the path of <code>pathToSearch</code>
   */
  public static boolean isMatchingTail(final Path pathToSearch, String pathTail) {
    return isMatchingTail(pathToSearch, new Path(pathTail));
  }

  /**
   * Compare path component of the Path URI; e.g. if hdfs://a/b/c and /a/b/c, it will compare the
   * '/a/b/c' part. If you passed in 'hdfs://a/b/c and b/c, it would return true.  Does not consider
   * schema; i.e. if schemas different but path or subpath matches, the two will equate.
   * @param pathToSearch Path we will be trying to match.
   * @param pathTail
   * @return True if <code>pathTail</code> is tail on the path of <code>pathToSearch</code>
   */
  public static boolean isMatchingTail(final Path pathToSearch, final Path pathTail) {
    if (pathToSearch.depth() != pathTail.depth()) return false;
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
      if (toSearchName == null || toSearchName.length() <= 0) break;
      // Move up a parent on each path for next go around.  Path doesn't let us go off the end.
      tailPath = tailPath.getParent();
      toSearch = toSearch.getParent();
    } while(tailName.equals(toSearchName));
    return result;
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
   * Delete the region directory if exists.
   * @param conf
   * @param hri
   * @return True if deleted the region directory.
   * @throws IOException
   */
  public static boolean deleteRegionDir(final Configuration conf, final HRegionInfo hri)
  throws IOException {
    Path rootDir = getRootDir(conf);
    FileSystem fs = rootDir.getFileSystem(conf);
    return deleteDirectory(fs,
      new Path(getTableDir(rootDir, hri.getTable()), hri.getEncodedName()));
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
   * <li>overwrite the file if it exists</li>
   * <li>apply the umask in the configuration (if it is enabled)</li>
   * <li>use the fs configured buffer size (or 4096 if not set)</li>
   * <li>use the configured column family replication or default replication if
   * {@link HColumnDescriptor#DEFAULT_DFS_REPLICATION}</li>
   * <li>use the default block size</li>
   * <li>not track progress</li>
   * </ol>
   * @param conf configurations
   * @param fs {@link FileSystem} on which to write the file
   * @param path {@link Path} to the file to write
   * @param perm permissions
   * @param favoredNodes
   * @return output stream to the created file
   * @throws IOException if the file cannot be created
   */
  public static FSDataOutputStream create(Configuration conf, FileSystem fs, Path path,
      FsPermission perm, InetSocketAddress[] favoredNodes) throws IOException {
    if (fs instanceof HFileSystem) {
      FileSystem backingFs = ((HFileSystem)fs).getBackingFs();
      if (backingFs instanceof DistributedFileSystem) {
        // Try to use the favoredNodes version via reflection to allow backwards-
        // compatibility.
        short replication = Short.parseShort(conf.get(HColumnDescriptor.DFS_REPLICATION,
          String.valueOf(HColumnDescriptor.DEFAULT_DFS_REPLICATION)));
        try {
          return (FSDataOutputStream) (DistributedFileSystem.class.getDeclaredMethod("create",
            Path.class, FsPermission.class, boolean.class, int.class, short.class, long.class,
            Progressable.class, InetSocketAddress[].class).invoke(backingFs, path, perm, true,
            getDefaultBufferSize(backingFs),
            replication > 0 ? replication : getDefaultReplication(backingFs, path),
            getDefaultBlockSize(backingFs, path), null, favoredNodes));
        } catch (InvocationTargetException ite) {
          // Function was properly called, but threw it's own exception.
          throw new IOException(ite.getCause());
        } catch (NoSuchMethodException e) {
          LOG.debug("DFS Client does not support most favored nodes create; using default create");
          if (LOG.isTraceEnabled()) LOG.trace("Ignoring; use default create", e);
        } catch (IllegalArgumentException e) {
          LOG.debug("Ignoring (most likely Reflection related exception) " + e);
        } catch (SecurityException e) {
          LOG.debug("Ignoring (most likely Reflection related exception) " + e);
        } catch (IllegalAccessException e) {
          LOG.debug("Ignoring (most likely Reflection related exception) " + e);
        }
      }
    }
    return create(fs, path, perm, true);
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
   * @param perm
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
        if (mask == null)
          return FsPermission.getFileDefault();
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
   * HdfsConstants.SafeModeAction action, boolean isChecked)} is not in hadoop 1.1
   *
   * @param dfs
   * @return whether we're in safe mode
   * @throws IOException
   */
  private static boolean isInSafeMode(DistributedFileSystem dfs) throws IOException {
    boolean inSafeMode = false;
    try {
      Method m = DistributedFileSystem.class.getMethod("setSafeMode", new Class<?> []{
          org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction.class, boolean.class});
      inSafeMode = (Boolean) m.invoke(dfs,
        org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction.SAFEMODE_GET, true);
    } catch (Exception e) {
      if (e instanceof IOException) throw (IOException) e;

      // Check whether dfs is on safemode.
      inSafeMode = dfs.setSafeMode(
        org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction.SAFEMODE_GET);
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
   * @throws org.apache.hadoop.hbase.exceptions.DeserializationException
   */
  public static String getVersion(FileSystem fs, Path rootdir)
  throws IOException, DeserializationException {
    Path versionFile = new Path(rootdir, HConstants.VERSION_FILE_NAME);
    FileStatus[] status = null;
    try {
      // hadoop 2.0 throws FNFE if directory does not exist.
      // hadoop 1.0 returns null if directory does not exist.
      status = fs.listStatus(versionFile);
    } catch (FileNotFoundException fnfe) {
      return null;
    }
    if (status == null || status.length == 0) return null;
    String version = null;
    byte [] content = new byte [(int)status[0].getLen()];
    FSDataInputStream s = fs.open(versionFile);
    try {
      IOUtils.readFully(s, content, 0, content.length);
      if (ProtobufUtil.isPBMagicPrefix(content)) {
        version = parseVersionFrom(content);
      } else {
        // Presume it pre-pb format.
        InputStream is = new ByteArrayInputStream(content);
        DataInputStream dis = new DataInputStream(is);
        try {
          version = dis.readUTF();
        } finally {
          dis.close();
        }
      }
    } catch (EOFException eof) {
      LOG.warn("Version file was empty, odd, will try to set it.");
    } finally {
      s.close();
    }
    return version;
  }

  /**
   * Parse the content of the ${HBASE_ROOTDIR}/hbase.version file.
   * @param bytes The byte content of the hbase.version file.
   * @return The version found in the file as a String.
   * @throws DeserializationException
   */
  static String parseVersionFrom(final byte [] bytes)
  throws DeserializationException {
    ProtobufUtil.expectPBMagicPrefix(bytes);
    int pblen = ProtobufUtil.lengthOfPBMagic();
    FSProtos.HBaseVersionFileContent.Builder builder =
      FSProtos.HBaseVersionFileContent.newBuilder();
    try {
      ProtobufUtil.mergeFrom(builder, bytes, pblen, bytes.length - pblen);
      return builder.getVersion();
    } catch (IOException e) {
      // Convert
      throw new DeserializationException(e);
    }
  }

  /**
   * Create the content to write into the ${HBASE_ROOTDIR}/hbase.version file.
   * @param version Version to persist
   * @return Serialized protobuf with <code>version</code> content and a bit of pb magic for a prefix.
   */
  static byte [] toVersionByteArray(final String version) {
    FSProtos.HBaseVersionFileContent.Builder builder =
      FSProtos.HBaseVersionFileContent.newBuilder();
    return ProtobufUtil.prependPBMagic(builder.setVersion(version).build().toByteArray());
  }

  /**
   * Verifies current version of file system
   *
   * @param fs file system
   * @param rootdir root directory of HBase installation
   * @param message if true, issues a message on System.out
   *
   * @throws IOException e
   * @throws DeserializationException
   */
  public static void checkVersion(FileSystem fs, Path rootdir, boolean message)
  throws IOException, DeserializationException {
    checkVersion(fs, rootdir, message, 0, HConstants.DEFAULT_VERSION_FILE_WRITE_ATTEMPTS);
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
   * @throws DeserializationException
   */
  public static void checkVersion(FileSystem fs, Path rootdir,
      boolean message, int wait, int retries)
  throws IOException, DeserializationException {
    String version = getVersion(fs, rootdir);
    if (version == null) {
      if (!metaRegionExists(fs, rootdir)) {
        // rootDir is empty (no version file and no root region)
        // just create new version file (HBASE-1195)
        setVersion(fs, rootdir, wait, retries);
        return;
      }
    } else if (version.compareTo(HConstants.FILE_SYSTEM_VERSION) == 0) return;

    // version is deprecated require migration
    // Output on stdout so user sees it in terminal.
    String msg = "HBase file layout needs to be upgraded."
      + " You have version " + version
      + " and I want version " + HConstants.FILE_SYSTEM_VERSION
      + ". Consult http://hbase.apache.org/book.html for further information about upgrading HBase."
      + " Is your hbase.rootdir valid? If so, you may need to run "
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
    Path tempVersionFile = new Path(rootdir, HConstants.HBASE_TEMP_DIRECTORY + Path.SEPARATOR +
      HConstants.VERSION_FILE_NAME);
    while (true) {
      try {
        // Write the version to a temporary file
        FSDataOutputStream s = fs.create(tempVersionFile);
        try {
          s.write(toVersionByteArray(version));
          s.close();
          s = null;
          // Move the temp version file to its normal location. Returns false
          // if the rename failed. Throw an IOE in that case.
          if (!fs.rename(tempVersionFile, versionFile)) {
            throw new IOException("Unable to move temp version file to " + versionFile);
          }
        } finally {
          // Cleaning up the temporary if the rename failed would be trying
          // too hard. We'll unconditionally create it again the next time
          // through anyway, files are overwritten by default by create().

          // Attempt to close the stream on the way out if it is still open.
          try {
            if (s != null) s.close();
          } catch (IOException ignore) { }
        }
        LOG.info("Created version file at " + rootdir.toString() + " with version=" + version);
        return;
      } catch (IOException e) {
        if (retries > 0) {
          LOG.debug("Unable to create version file at " + rootdir.toString() + ", retrying", e);
          fs.delete(versionFile, false);
          try {
            if (wait > 0) {
              Thread.sleep(wait);
            }
          } catch (InterruptedException ie) {
            throw (InterruptedIOException)new InterruptedIOException().initCause(ie);
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
          } catch (InterruptedException e) {
            throw (InterruptedIOException)new InterruptedIOException().initCause(e);
          }
        } else {
          throw ioe;
        }
      }
    }
  }

  /**
   * Returns the value of the unique cluster ID stored for this HBase instance.
   * @param fs the root directory FileSystem
   * @param rootdir the path to the HBase root directory
   * @return the unique cluster identifier
   * @throws IOException if reading the cluster ID file fails
   */
  public static ClusterId getClusterId(FileSystem fs, Path rootdir)
  throws IOException {
    Path idPath = new Path(rootdir, HConstants.CLUSTER_ID_FILE_NAME);
    ClusterId clusterId = null;
    FileStatus status = fs.exists(idPath)? fs.getFileStatus(idPath):  null;
    if (status != null) {
      int len = Ints.checkedCast(status.getLen());
      byte [] content = new byte[len];
      FSDataInputStream in = fs.open(idPath);
      try {
        in.readFully(content);
      } catch (EOFException eof) {
        LOG.warn("Cluster ID file " + idPath.toString() + " was empty");
      } finally{
        in.close();
      }
      try {
        clusterId = ClusterId.parseFrom(content);
      } catch (DeserializationException e) {
        throw new IOException("content=" + Bytes.toString(content), e);
      }
      // If not pb'd, make it so.
      if (!ProtobufUtil.isPBMagicPrefix(content)) {
        String cid = null;
        in = fs.open(idPath);
        try {
          cid = in.readUTF();
          clusterId = new ClusterId(cid);
        } catch (EOFException eof) {
          LOG.warn("Cluster ID file " + idPath.toString() + " was empty");
        } finally {
          in.close();
        }
        rewriteAsPb(fs, rootdir, idPath, clusterId);
      }
      return clusterId;
    } else {
      LOG.warn("Cluster ID file does not exist at " + idPath.toString());
    }
    return clusterId;
  }

  /**
   * @param cid
   * @throws IOException
   */
  private static void rewriteAsPb(final FileSystem fs, final Path rootdir, final Path p,
      final ClusterId cid)
  throws IOException {
    // Rewrite the file as pb.  Move aside the old one first, write new
    // then delete the moved-aside file.
    Path movedAsideName = new Path(p + "." + System.currentTimeMillis());
    if (!fs.rename(p, movedAsideName)) throw new IOException("Failed rename of " + p);
    setClusterId(fs, rootdir, cid, 100);
    if (!fs.delete(movedAsideName, false)) {
      throw new IOException("Failed delete of " + movedAsideName);
    }
    LOG.debug("Rewrote the hbase.id file as pb");
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
  public static void setClusterId(FileSystem fs, Path rootdir, ClusterId clusterId,
      int wait) throws IOException {
    while (true) {
      try {
        Path idFile = new Path(rootdir, HConstants.CLUSTER_ID_FILE_NAME);
        Path tempIdFile = new Path(rootdir, HConstants.HBASE_TEMP_DIRECTORY +
          Path.SEPARATOR + HConstants.CLUSTER_ID_FILE_NAME);
        // Write the id file to a temporary location
        FSDataOutputStream s = fs.create(tempIdFile);
        try {
          s.write(clusterId.toByteArray());
          s.close();
          s = null;
          // Move the temporary file to its normal location. Throw an IOE if
          // the rename failed
          if (!fs.rename(tempIdFile, idFile)) {
            throw new IOException("Unable to move temp version file to " + idFile);
          }
        } finally {
          // Attempt to close the stream if still open on the way out
          try {
            if (s != null) s.close();
          } catch (IOException ignore) { }
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Created cluster ID file at " + idFile.toString() + " with ID: " + clusterId);
        }
        return;
      } catch (IOException ioe) {
        if (wait > 0) {
          LOG.warn("Unable to create cluster ID file in " + rootdir.toString() +
              ", retrying in " + wait + "msec: " + StringUtils.stringifyException(ioe));
          try {
            Thread.sleep(wait);
          } catch (InterruptedException e) {
            throw (InterruptedIOException)new InterruptedIOException().initCause(e);
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
   * Checks for the presence of the root path (using the provided conf object) in the given path. If
   * it exists, this method removes it and returns the String representation of remaining relative path.
   * @param path
   * @param conf
   * @return String representation of the remaining relative path
   * @throws IOException
   */
  public static String removeRootPath(Path path, final Configuration conf) throws IOException {
    Path root = FSUtils.getRootDir(conf);
    String pathStr = path.toString();
    // check that the path is absolute... it has the root path in it.
    if (!pathStr.startsWith(root.toString())) return pathStr;
    // if not, return as it is.
    return pathStr.substring(root.toString().length() + 1);// remove the "/" too.
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
        throw (InterruptedIOException)new InterruptedIOException().initCause(e);
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
  }

  /**
   * Checks if meta region exists
   *
   * @param fs file system
   * @param rootdir root directory of HBase installation
   * @return true if exists
   * @throws IOException e
   */
  @SuppressWarnings("deprecation")
  public static boolean metaRegionExists(FileSystem fs, Path rootdir)
  throws IOException {
    Path metaRegionDir =
      HRegion.getRegionDir(rootdir, HRegionInfo.FIRST_META_REGIONINFO);
    return fs.exists(metaRegionDir);
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

  // TODO move this method OUT of FSUtils. No dependencies to HMaster
  /**
   * Returns the total overall fragmentation percentage. Includes hbase:meta and
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
   * have more than one file in them. Checks -ROOT- and hbase:meta too. The total
   * percentage across all tables is stored under the special key "-TOTAL-".
   *
   * @param master  The master defining the HBase root and file system.
   * @return A map for each table and its percentage.
   *
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
   * have more than one file in them. Checks -ROOT- and hbase:meta too. The total
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
    PathFilter regionFilter = new RegionDirFilter(fs);
    PathFilter familyFilter = new FamilyDirFilter(fs);
    List<Path> tableDirs = getTableDirs(fs, hbaseRootDir);
    for (Path d : tableDirs) {
      int cfCount = 0;
      int cfFrag = 0;
      FileStatus[] regionDirs = fs.listStatus(d, regionFilter);
      for (FileStatus regionDir : regionDirs) {
        Path dd = regionDir.getPath();
        // else its a region name, now look in region for families
        FileStatus[] familyDirs = fs.listStatus(dd, familyFilter);
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
      frags.put(FSUtils.getTableName(d).getNameAsString(),
        cfCount == 0? 0: Math.round((float) cfFrag / cfCount * 100));
    }
    // set overall percentage for all tables
    frags.put("-TOTAL-",
      cfCountTotal == 0? 0: Math.round((float) cfFragTotal / cfCountTotal * 100));
    return frags;
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
   * Directory filter that doesn't include any of the directories in the specified blacklist
   */
  public static class BlackListDirFilter implements PathFilter {
    private final FileSystem fs;
    private List<String> blacklist;

    /**
     * Create a filter on the givem filesystem with the specified blacklist
     * @param fs filesystem to filter
     * @param directoryNameBlackList list of the names of the directories to filter. If
     *          <tt>null</tt>, all directories are returned
     */
    @SuppressWarnings("unchecked")
    public BlackListDirFilter(final FileSystem fs, final List<String> directoryNameBlackList) {
      this.fs = fs;
      blacklist =
        (List<String>) (directoryNameBlackList == null ? Collections.emptyList()
          : directoryNameBlackList);
    }

    @Override
    public boolean accept(Path p) {
      boolean isValid = false;
      try {
        if (isValidName(p.getName())) {
          isValid = fs.getFileStatus(p).isDirectory();
        } else {
          isValid = false;
        }
      } catch (IOException e) {
        LOG.warn("An error occurred while verifying if [" + p.toString()
            + "] is a valid directory. Returning 'not valid' and continuing.", e);
      }
      return isValid;
    }

    protected boolean isValidName(final String name) {
      return !blacklist.contains(name);
    }
  }

  /**
   * A {@link PathFilter} that only allows directories.
   */
  public static class DirFilter extends BlackListDirFilter {

    public DirFilter(FileSystem fs) {
      super(fs, null);
    }
  }

  /**
   * A {@link PathFilter} that returns usertable directories. To get all directories use the
   * {@link BlackListDirFilter} with a <tt>null</tt> blacklist
   */
  public static class UserTableDirFilter extends BlackListDirFilter {
    public UserTableDirFilter(FileSystem fs) {
      super(fs, HConstants.HBASE_NON_TABLE_DIRS);
    }

    protected boolean isValidName(final String name) {
      if (!super.isValidName(name))
        return false;

      try {
        TableName.isLegalTableQualifierName(Bytes.toBytes(name));
      } catch (IllegalArgumentException e) {
        LOG.info("INVALID NAME " + name);
        return false;
      }
      return true;
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
      Configuration conf, CancelableProgressable reporter) throws IOException;

  public static List<Path> getTableDirs(final FileSystem fs, final Path rootdir)
      throws IOException {
    List<Path> tableDirs = new LinkedList<Path>();

    for(FileStatus status :
        fs.globStatus(new Path(rootdir,
            new Path(HConstants.BASE_NAMESPACE_DIR, "*")))) {
      tableDirs.addAll(FSUtils.getLocalTableDirs(fs, status.getPath()));
    }
    return tableDirs;
  }

  /**
   * @param fs
   * @param rootdir
   * @return All the table directories under <code>rootdir</code>. Ignore non table hbase folders such as
   * .logs, .oldlogs, .corrupt folders.
   * @throws IOException
   */
  public static List<Path> getLocalTableDirs(final FileSystem fs, final Path rootdir)
      throws IOException {
    // presumes any directory under hbase.rootdir is a table
    FileStatus[] dirs = fs.listStatus(rootdir, new UserTableDirFilter(fs));
    List<Path> tabledirs = new ArrayList<Path>(dirs.length);
    for (FileStatus dir: dirs) {
      tabledirs.add(dir.getPath());
    }
    return tabledirs;
  }

  /**
   * Checks if the given path is the one with 'recovered.edits' dir.
   * @param path
   * @return True if we recovered edits
   */
  public static boolean isRecoveredEdits(Path path) {
    return path.toString().contains(HConstants.RECOVERED_EDITS_DIR);
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
        return fs.getFileStatus(rd).isDirectory();
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
   * @param tableDir Path to a specific table directory &lt;hbase.rootdir&gt;/&lt;tabledir&gt;
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
   * dirs &lt;hbase.rootdir&gt;/&lt;tabledir&gt;/&lt;regiondir&gt;/&lt;colfamdir&gt;.
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
        return fs.getFileStatus(rd).isDirectory();
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

  public static List<Path> getReferenceFilePaths(final FileSystem fs, final Path familyDir) throws IOException {
    FileStatus[] fds = fs.listStatus(familyDir, new ReferenceFileFilter(fs));
    List<Path> referenceFiles = new ArrayList<Path>(fds.length);
    for (FileStatus fdfs: fds) {
      Path fdPath = fdfs.getPath();
      referenceFiles.add(fdPath);
    }
    return referenceFiles;
  }

  /**
   * Filter for HFiles that excludes reference files.
   */
  public static class HFileFilter implements PathFilter {
    final FileSystem fs;

    public HFileFilter(FileSystem fs) {
      this.fs = fs;
    }

    @Override
    public boolean accept(Path rd) {
      try {
        // only files
        return !fs.getFileStatus(rd).isDirectory() && StoreFileInfo.isHFile(rd);
      } catch (IOException ioe) {
        // Maybe the file was moved or the fs was disconnected.
        LOG.warn("Skipping file " + rd +" due to IOException", ioe);
        return false;
      }
    }
  }

  public static class ReferenceFileFilter implements PathFilter {

    private final FileSystem fs;

    public ReferenceFileFilter(FileSystem fs) {
      this.fs = fs;
    }

    @Override
    public boolean accept(Path rd) {
      try {
        // only files can be references.
        return !fs.getFileStatus(rd).isDirectory() && StoreFileInfo.isReference(rd);
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
   * Runs through the HBase rootdir/tablename and creates a reverse lookup map for
   * table StoreFile names to the full Path.
   * <br>
   * Example...<br>
   * Key = 3944417774205889744  <br>
   * Value = hdfs://localhost:51169/user/userid/-ROOT-/70236052/info/3944417774205889744
   *
   * @param map map to add values.  If null, this method will create and populate one to return
   * @param fs  The file system to use.
   * @param hbaseRootDir  The root directory to scan.
   * @param tableName name of the table to scan.
   * @return Map keyed by StoreFile name with a value of the full Path.
   * @throws IOException When scanning the directory fails.
   */
  public static Map<String, Path> getTableStoreFilePathMap(Map<String, Path> map,
  final FileSystem fs, final Path hbaseRootDir, TableName tableName)
  throws IOException {
    return getTableStoreFilePathMap(map, fs, hbaseRootDir, tableName, null);
  }

  /**
   * Runs through the HBase rootdir/tablename and creates a reverse lookup map for
   * table StoreFile names to the full Path.
   * <br>
   * Example...<br>
   * Key = 3944417774205889744  <br>
   * Value = hdfs://localhost:51169/user/userid/-ROOT-/70236052/info/3944417774205889744
   *
   * @param map map to add values.  If null, this method will create and populate one to return
   * @param fs  The file system to use.
   * @param hbaseRootDir  The root directory to scan.
   * @param tableName name of the table to scan.
   * @param errors ErrorReporter instance or null
   * @return Map keyed by StoreFile name with a value of the full Path.
   * @throws IOException When scanning the directory fails.
   */
  public static Map<String, Path> getTableStoreFilePathMap(Map<String, Path> map,
  final FileSystem fs, final Path hbaseRootDir, TableName tableName, ErrorReporter errors)
  throws IOException {
    if (map == null) {
      map = new HashMap<String, Path>();
    }

    // only include the directory paths to tables
    Path tableDir = FSUtils.getTableDir(hbaseRootDir, tableName);
    // Inside a table, there are compaction.dir directories to skip.  Otherwise, all else
    // should be regions.
    PathFilter familyFilter = new FamilyDirFilter(fs);
    FileStatus[] regionDirs = fs.listStatus(tableDir, new RegionDirFilter(fs));
    for (FileStatus regionDir : regionDirs) {
      if (null != errors) {
        errors.progress();
      }
      Path dd = regionDir.getPath();
      // else its a region name, now look in region for families
      FileStatus[] familyDirs = fs.listStatus(dd, familyFilter);
      for (FileStatus familyDir : familyDirs) {
        if (null != errors) {
          errors.progress();
        }
        Path family = familyDir.getPath();
        if (family.getName().equals(HConstants.RECOVERED_EDITS_DIR)) {
          continue;
        }
        // now in family, iterate over the StoreFiles and
        // put in map
        FileStatus[] familyStatus = fs.listStatus(family);
        for (FileStatus sfStatus : familyStatus) {
          if (null != errors) {
            errors.progress();
          }
          Path sf = sfStatus.getPath();
          map.put( sf.getName(), sf);
        }
      }
    }
    return map;
  }

  public static int getRegionReferenceFileCount(final FileSystem fs, final Path p) {
    int result = 0;
    try {
      for (Path familyDir:getFamilyDirs(fs, p)){
        result += getReferenceFilePaths(fs, familyDir).size();
      }
    } catch (IOException e) {
      LOG.warn("Error Counting reference files.", e);
    }
    return result;
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
    return getTableStoreFilePathMap(fs, hbaseRootDir, null);
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
   * @param errors ErrorReporter instance or null
   * @return Map keyed by StoreFile name with a value of the full Path.
   * @throws IOException When scanning the directory fails.
   */
  public static Map<String, Path> getTableStoreFilePathMap(
    final FileSystem fs, final Path hbaseRootDir, ErrorReporter errors)
  throws IOException {
    Map<String, Path> map = new HashMap<String, Path>();

    // if this method looks similar to 'getTableFragmentation' that is because
    // it was borrowed from it.

    // only include the directory paths to tables
    for (Path tableDir : FSUtils.getTableDirs(fs, hbaseRootDir)) {
      getTableStoreFilePathMap(map, fs, hbaseRootDir,
          FSUtils.getTableName(tableDir), errors);
    }
    return map;
  }

  /**
   * Calls fs.listStatus() and treats FileNotFoundException as non-fatal
   * This accommodates differences between hadoop versions, where hadoop 1
   * does not throw a FileNotFoundException, and return an empty FileStatus[]
   * while Hadoop 2 will throw FileNotFoundException.
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
   * @return the value returned by the fs.delete()
   * @throws IOException
   */
  public static boolean delete(final FileSystem fs, final Path path, final boolean recursive)
      throws IOException {
    return fs.delete(path, recursive);
  }

  /**
   * Calls fs.exists(). Checks if the specified path exists
   *
   * @param fs
   * @param path
   * @return the value returned by fs.exists()
   * @throws IOException
   */
  public static boolean isExists(final FileSystem fs, final Path path) throws IOException {
    return fs.exists(path);
  }

  /**
   * Throw an exception if an action is not permitted by a user on a file.
   *
   * @param ugi
   *          the user
   * @param file
   *          the file
   * @param action
   *          the action
   */
  public static void checkAccess(UserGroupInformation ugi, FileStatus file,
      FsAction action) throws AccessDeniedException {
    if (ugi.getShortUserName().equals(file.getOwner())) {
      if (file.getPermission().getUserAction().implies(action)) {
        return;
      }
    } else if (contains(ugi.getGroupNames(), file.getGroup())) {
      if (file.getPermission().getGroupAction().implies(action)) {
        return;
      }
    } else if (file.getPermission().getOtherAction().implies(action)) {
      return;
    }
    throw new AccessDeniedException("Permission denied:" + " action=" + action
        + " path=" + file.getPath() + " user=" + ugi.getShortUserName());
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
   *
   * @see #logFileSystemState(FileSystem, Path, Log)
   */
  private static void logFSTree(Log LOG, final FileSystem fs, final Path root, String prefix)
      throws IOException {
    FileStatus[] files = FSUtils.listStatus(fs, root, null);
    if (files == null) return;

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
   * This function is to scan the root path of the file system to get the
   * degree of locality for each region on each of the servers having at least
   * one block of that region.
   * This is used by the tool {@link org.apache.hadoop.hbase.master.RegionPlacementMaintainer}
   *
   * @param conf
   *          the configuration to use
   * @return the mapping from region encoded name to a map of server names to
   *           locality fraction
   * @throws IOException
   *           in case of file system errors or interrupts
   */
  public static Map<String, Map<String, Float>> getRegionDegreeLocalityMappingFromFS(
      final Configuration conf) throws IOException {
    return getRegionDegreeLocalityMappingFromFS(
        conf, null,
        conf.getInt(THREAD_POOLSIZE, DEFAULT_THREAD_POOLSIZE));

  }

  /**
   * This function is to scan the root path of the file system to get the
   * degree of locality for each region on each of the servers having at least
   * one block of that region.
   *
   * @param conf
   *          the configuration to use
   * @param desiredTable
   *          the table you wish to scan locality for
   * @param threadPoolSize
   *          the thread pool size to use
   * @return the mapping from region encoded name to a map of server names to
   *           locality fraction
   * @throws IOException
   *           in case of file system errors or interrupts
   */
  public static Map<String, Map<String, Float>> getRegionDegreeLocalityMappingFromFS(
      final Configuration conf, final String desiredTable, int threadPoolSize)
      throws IOException {
    Map<String, Map<String, Float>> regionDegreeLocalityMapping =
        new ConcurrentHashMap<String, Map<String, Float>>();
    getRegionLocalityMappingFromFS(conf, desiredTable, threadPoolSize, null,
        regionDegreeLocalityMapping);
    return regionDegreeLocalityMapping;
  }

  /**
   * This function is to scan the root path of the file system to get either the
   * mapping between the region name and its best locality region server or the
   * degree of locality of each region on each of the servers having at least
   * one block of that region. The output map parameters are both optional.
   *
   * @param conf
   *          the configuration to use
   * @param desiredTable
   *          the table you wish to scan locality for
   * @param threadPoolSize
   *          the thread pool size to use
   * @param regionToBestLocalityRSMapping
   *          the map into which to put the best locality mapping or null
   * @param regionDegreeLocalityMapping
   *          the map into which to put the locality degree mapping or null,
   *          must be a thread-safe implementation
   * @throws IOException
   *           in case of file system errors or interrupts
   */
  private static void getRegionLocalityMappingFromFS(
      final Configuration conf, final String desiredTable,
      int threadPoolSize,
      Map<String, String> regionToBestLocalityRSMapping,
      Map<String, Map<String, Float>> regionDegreeLocalityMapping)
      throws IOException {
    FileSystem fs =  FileSystem.get(conf);
    Path rootPath = FSUtils.getRootDir(conf);
    long startTime = EnvironmentEdgeManager.currentTime();
    Path queryPath;
    // The table files are in ${hbase.rootdir}/data/<namespace>/<table>/*
    if (null == desiredTable) {
      queryPath = new Path(new Path(rootPath, HConstants.BASE_NAMESPACE_DIR).toString() + "/*/*/*/");
    } else {
      queryPath = new Path(FSUtils.getTableDir(rootPath, TableName.valueOf(desiredTable)).toString() + "/*/");
    }

    // reject all paths that are not appropriate
    PathFilter pathFilter = new PathFilter() {
      @Override
      public boolean accept(Path path) {
        // this is the region name; it may get some noise data
        if (null == path) {
          return false;
        }

        // no parent?
        Path parent = path.getParent();
        if (null == parent) {
          return false;
        }

        String regionName = path.getName();
        if (null == regionName) {
          return false;
        }

        if (!regionName.toLowerCase(Locale.ROOT).matches("[0-9a-f]+")) {
          return false;
        }
        return true;
      }
    };

    FileStatus[] statusList = fs.globStatus(queryPath, pathFilter);

    if (null == statusList) {
      return;
    } else {
      LOG.debug("Query Path: " + queryPath + " ; # list of files: " +
          statusList.length);
    }

    // lower the number of threads in case we have very few expected regions
    threadPoolSize = Math.min(threadPoolSize, statusList.length);

    // run in multiple threads
    ThreadPoolExecutor tpe = new ThreadPoolExecutor(threadPoolSize,
        threadPoolSize, 60, TimeUnit.SECONDS,
        new ArrayBlockingQueue<Runnable>(statusList.length));
    try {
      // ignore all file status items that are not of interest
      for (FileStatus regionStatus : statusList) {
        if (null == regionStatus) {
          continue;
        }

        if (!regionStatus.isDirectory()) {
          continue;
        }

        Path regionPath = regionStatus.getPath();
        if (null == regionPath) {
          continue;
        }

        tpe.execute(new FSRegionScanner(fs, regionPath,
            regionToBestLocalityRSMapping, regionDegreeLocalityMapping));
      }
    } finally {
      tpe.shutdown();
      int threadWakeFrequency = conf.getInt(HConstants.THREAD_WAKE_FREQUENCY,
          60 * 1000);
      try {
        // here we wait until TPE terminates, which is either naturally or by
        // exceptions in the execution of the threads
        while (!tpe.awaitTermination(threadWakeFrequency,
            TimeUnit.MILLISECONDS)) {
          // printing out rough estimate, so as to not introduce
          // AtomicInteger
          LOG.info("Locality checking is underway: { Scanned Regions : "
              + tpe.getCompletedTaskCount() + "/"
              + tpe.getTaskCount() + " }");
        }
      } catch (InterruptedException e) {
        throw (InterruptedIOException)new InterruptedIOException().initCause(e);
      }
    }

    long overhead = EnvironmentEdgeManager.currentTime() - startTime;
    String overheadMsg = "Scan DFS for locality info takes " + overhead + " ms";

    LOG.info(overheadMsg);
  }

  /**
   * Do our short circuit read setup.
   * Checks buffer size to use and whether to do checksumming in hbase or hdfs.
   * @param conf
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
   * @param conf
   */
  public static void checkShortCircuitReadBufferSize(final Configuration conf) {
    final int defaultSize = HConstants.DEFAULT_BLOCKSIZE * 2;
    final int notSet = -1;
    // DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_BUFFER_SIZE_KEY is only defined in h2
    final String dfsKey = "dfs.client.read.shortcircuit.buffer.size";
    int size = conf.getInt(dfsKey, notSet);
    // If a size is set, return -- we will use it.
    if (size != notSet) return;
    // But short circuit buffer size is normally not set.  Put in place the hbase wanted size.
    int hbaseSize = conf.getInt("hbase." + dfsKey, defaultSize);
    conf.setIfUnset(dfsKey, Integer.toString(hbaseSize));
  }
}
