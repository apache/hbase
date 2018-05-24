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

import edu.umd.cs.findbugs.annotations.CheckForNull;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.ClusterId;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.util.HBaseFsck.ErrorReporter;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSHedgedReadMetrics;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hbase.thirdparty.com.google.common.base.Throwables;
import org.apache.hbase.thirdparty.com.google.common.collect.Iterators;
import org.apache.hbase.thirdparty.com.google.common.primitives.Ints;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.FSProtos;

/**
 * Utility methods for interacting with the underlying file system.
 */
@InterfaceAudience.Private
public abstract class FSUtils extends CommonFSUtils {
  private static final Logger LOG = LoggerFactory.getLogger(FSUtils.class);

  private static final String THREAD_POOLSIZE = "hbase.client.localityCheck.threadPoolSize";
  private static final int DEFAULT_THREAD_POOLSIZE = 2;

  /** Set to true on Windows platforms */
  @VisibleForTesting // currently only used in testing. TODO refactor into a test class
  public static final boolean WINDOWS = System.getProperty("os.name").startsWith("Windows");

  protected FSUtils() {
    super();
  }

  /**
   * @return True is <code>fs</code> is instance of DistributedFileSystem
   * @throws IOException
   */
  public static boolean isDistributedFileSystem(final FileSystem fs) throws IOException {
    FileSystem fileSystem = fs;
    // If passed an instance of HFileSystem, it fails instanceof DistributedFileSystem.
    // Check its backing fs for dfs-ness.
    if (fs instanceof HFileSystem) {
      fileSystem = ((HFileSystem)fs).getBackingFs();
    }
    return fileSystem instanceof DistributedFileSystem;
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
      exception = e instanceof RemoteException ?
              ((RemoteException)e).unwrapRemoteException() : e;
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

  /**
   * Update blocksDistribution with blockLocations
   * @param blocksDistribution the hdfs blocks distribution
   * @param blockLocations an array containing block location
   */
  static public void addToHDFSBlocksDistribution(
      HDFSBlocksDistribution blocksDistribution, BlockLocation[] blockLocations)
      throws IOException {
    for (BlockLocation bl : blockLocations) {
      String[] hosts = bl.getHosts();
      long len = bl.getLength();
      blocksDistribution.addHostsAndBlockWeight(hosts, len);
    }
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
    Map<String, Integer> frags = new HashMap<>();
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

  public static void renameFile(FileSystem fs, Path src, Path dst) throws IOException {
    if (fs.exists(dst) && !fs.delete(dst, false)) {
      throw new IOException("Can not delete " + dst);
    }
    if (!fs.rename(src, dst)) {
      throw new IOException("Can not rename from " + src + " to " + dst);
    }
  }

  /**
   * A {@link PathFilter} that returns only regular files.
   */
  static class FileFilter extends AbstractFileStatusFilter {
    private final FileSystem fs;

    public FileFilter(final FileSystem fs) {
      this.fs = fs;
    }

    @Override
    protected boolean accept(Path p, @CheckForNull Boolean isDir) {
      try {
        return isFile(fs, isDir, p);
      } catch (IOException e) {
        LOG.warn("unable to verify if path=" + p + " is a regular file", e);
        return false;
      }
    }
  }

  /**
   * Directory filter that doesn't include any of the directories in the specified blacklist
   */
  public static class BlackListDirFilter extends AbstractFileStatusFilter {
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
    protected boolean accept(Path p, @CheckForNull Boolean isDir) {
      if (!isValidName(p.getName())) {
        return false;
      }

      try {
        return isDirectory(fs, isDir, p);
      } catch (IOException e) {
        LOG.warn("An error occurred while verifying if [" + p.toString()
            + "] is a valid directory. Returning 'not valid' and continuing.", e);
        return false;
      }
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

    @Override
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

  public void recoverFileLease(final FileSystem fs, final Path p, Configuration conf)
      throws IOException {
    recoverFileLease(fs, p, conf, null);
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
    List<Path> tableDirs = new LinkedList<>();

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
    List<Path> tabledirs = new ArrayList<>(dirs.length);
    for (FileStatus dir: dirs) {
      tabledirs.add(dir.getPath());
    }
    return tabledirs;
  }

  /**
   * Filter for all dirs that don't start with '.'
   */
  public static class RegionDirFilter extends AbstractFileStatusFilter {
    // This pattern will accept 0.90+ style hex region dirs and older numeric region dir names.
    final public static Pattern regionDirPattern = Pattern.compile("^[0-9a-f]*$");
    final FileSystem fs;

    public RegionDirFilter(FileSystem fs) {
      this.fs = fs;
    }

    @Override
    protected boolean accept(Path p, @CheckForNull Boolean isDir) {
      if (!regionDirPattern.matcher(p.getName()).matches()) {
        return false;
      }

      try {
        return isDirectory(fs, isDir, p);
      } catch (IOException ioe) {
        // Maybe the file was moved or the fs was disconnected.
        LOG.warn("Skipping file " + p +" due to IOException", ioe);
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
    List<FileStatus> rds = listStatusWithStatusFilter(fs, tableDir, new RegionDirFilter(fs));
    if (rds == null) {
      return new ArrayList<>();
    }
    List<Path> regionDirs = new ArrayList<>(rds.size());
    for (FileStatus rdfs: rds) {
      Path rdPath = rdfs.getPath();
      regionDirs.add(rdPath);
    }
    return regionDirs;
  }

  public static Path getRegionDir(Path tableDir, RegionInfo region) {
    return new Path(tableDir, ServerRegionReplicaUtil.getRegionInfoForFs(region).getEncodedName());
  }

  /**
   * Filter for all dirs that are legal column family names.  This is generally used for colfam
   * dirs &lt;hbase.rootdir&gt;/&lt;tabledir&gt;/&lt;regiondir&gt;/&lt;colfamdir&gt;.
   */
  public static class FamilyDirFilter extends AbstractFileStatusFilter {
    final FileSystem fs;

    public FamilyDirFilter(FileSystem fs) {
      this.fs = fs;
    }

    @Override
    protected boolean accept(Path p, @CheckForNull Boolean isDir) {
      try {
        // throws IAE if invalid
        HColumnDescriptor.isLegalFamilyName(Bytes.toBytes(p.getName()));
      } catch (IllegalArgumentException iae) {
        // path name is an invalid family name and thus is excluded.
        return false;
      }

      try {
        return isDirectory(fs, isDir, p);
      } catch (IOException ioe) {
        // Maybe the file was moved or the fs was disconnected.
        LOG.warn("Skipping file " + p +" due to IOException", ioe);
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
    List<Path> familyDirs = new ArrayList<>(fds.length);
    for (FileStatus fdfs: fds) {
      Path fdPath = fdfs.getPath();
      familyDirs.add(fdPath);
    }
    return familyDirs;
  }

  public static List<Path> getReferenceFilePaths(final FileSystem fs, final Path familyDir) throws IOException {
    List<FileStatus> fds = listStatusWithStatusFilter(fs, familyDir, new ReferenceFileFilter(fs));
    if (fds == null) {
      return new ArrayList<>();
    }
    List<Path> referenceFiles = new ArrayList<>(fds.size());
    for (FileStatus fdfs: fds) {
      Path fdPath = fdfs.getPath();
      referenceFiles.add(fdPath);
    }
    return referenceFiles;
  }

  /**
   * Filter for HFiles that excludes reference files.
   */
  public static class HFileFilter extends AbstractFileStatusFilter {
    final FileSystem fs;

    public HFileFilter(FileSystem fs) {
      this.fs = fs;
    }

    @Override
    protected boolean accept(Path p, @CheckForNull Boolean isDir) {
      if (!StoreFileInfo.isHFile(p)) {
        return false;
      }

      try {
        return isFile(fs, isDir, p);
      } catch (IOException ioe) {
        // Maybe the file was moved or the fs was disconnected.
        LOG.warn("Skipping file " + p +" due to IOException", ioe);
        return false;
      }
    }
  }

  /**
   * Filter for HFileLinks (StoreFiles and HFiles not included).
   * the filter itself does not consider if a link is file or not.
   */
  public static class HFileLinkFilter implements PathFilter {

    @Override
    public boolean accept(Path p) {
      return HFileLink.isHFileLink(p);
    }
  }

  public static class ReferenceFileFilter extends AbstractFileStatusFilter {

    private final FileSystem fs;

    public ReferenceFileFilter(FileSystem fs) {
      this.fs = fs;
    }

    @Override
    protected boolean accept(Path p, @CheckForNull Boolean isDir) {
      if (!StoreFileInfo.isReference(p)) {
        return false;
      }

      try {
        // only files can be references.
        return isFile(fs, isDir, p);
      } catch (IOException ioe) {
        // Maybe the file was moved or the fs was disconnected.
        LOG.warn("Skipping file " + p +" due to IOException", ioe);
        return false;
      }
    }
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
   * @throws InterruptedException
   */
  public static Map<String, Path> getTableStoreFilePathMap(Map<String, Path> map,
  final FileSystem fs, final Path hbaseRootDir, TableName tableName)
  throws IOException, InterruptedException {
    return getTableStoreFilePathMap(map, fs, hbaseRootDir, tableName, null, null, null);
  }

  /**
   * Runs through the HBase rootdir/tablename and creates a reverse lookup map for
   * table StoreFile names to the full Path.  Note that because this method can be called
   * on a 'live' HBase system that we will skip files that no longer exist by the time
   * we traverse them and similarly the user of the result needs to consider that some
   * entries in this map may not exist by the time this call completes.
   * <br>
   * Example...<br>
   * Key = 3944417774205889744  <br>
   * Value = hdfs://localhost:51169/user/userid/-ROOT-/70236052/info/3944417774205889744
   *
   * @param resultMap map to add values.  If null, this method will create and populate one to return
   * @param fs  The file system to use.
   * @param hbaseRootDir  The root directory to scan.
   * @param tableName name of the table to scan.
   * @param sfFilter optional path filter to apply to store files
   * @param executor optional executor service to parallelize this operation
   * @param errors ErrorReporter instance or null
   * @return Map keyed by StoreFile name with a value of the full Path.
   * @throws IOException When scanning the directory fails.
   * @throws InterruptedException
   */
  public static Map<String, Path> getTableStoreFilePathMap(
      Map<String, Path> resultMap,
      final FileSystem fs, final Path hbaseRootDir, TableName tableName, final PathFilter sfFilter,
      ExecutorService executor, final ErrorReporter errors) throws IOException, InterruptedException {

    final Map<String, Path> finalResultMap =
        resultMap == null ? new ConcurrentHashMap<>(128, 0.75f, 32) : resultMap;

    // only include the directory paths to tables
    Path tableDir = FSUtils.getTableDir(hbaseRootDir, tableName);
    // Inside a table, there are compaction.dir directories to skip.  Otherwise, all else
    // should be regions.
    final FamilyDirFilter familyFilter = new FamilyDirFilter(fs);
    final Vector<Exception> exceptions = new Vector<>();

    try {
      List<FileStatus> regionDirs = FSUtils.listStatusWithStatusFilter(fs, tableDir, new RegionDirFilter(fs));
      if (regionDirs == null) {
        return finalResultMap;
      }

      final List<Future<?>> futures = new ArrayList<>(regionDirs.size());

      for (FileStatus regionDir : regionDirs) {
        if (null != errors) {
          errors.progress();
        }
        final Path dd = regionDir.getPath();

        if (!exceptions.isEmpty()) {
          break;
        }

        Runnable getRegionStoreFileMapCall = new Runnable() {
          @Override
          public void run() {
            try {
              HashMap<String,Path> regionStoreFileMap = new HashMap<>();
              List<FileStatus> familyDirs = FSUtils.listStatusWithStatusFilter(fs, dd, familyFilter);
              if (familyDirs == null) {
                if (!fs.exists(dd)) {
                  LOG.warn("Skipping region because it no longer exists: " + dd);
                } else {
                  LOG.warn("Skipping region because it has no family dirs: " + dd);
                }
                return;
              }
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
                  if (sfFilter == null || sfFilter.accept(sf)) {
                    regionStoreFileMap.put( sf.getName(), sf);
                  }
                }
              }
              finalResultMap.putAll(regionStoreFileMap);
            } catch (Exception e) {
              LOG.error("Could not get region store file map for region: " + dd, e);
              exceptions.add(e);
            }
          }
        };

        // If executor is available, submit async tasks to exec concurrently, otherwise
        // just do serial sync execution
        if (executor != null) {
          Future<?> future = executor.submit(getRegionStoreFileMapCall);
          futures.add(future);
        } else {
          FutureTask<?> future = new FutureTask<>(getRegionStoreFileMapCall, null);
          future.run();
          futures.add(future);
        }
      }

      // Ensure all pending tasks are complete (or that we run into an exception)
      for (Future<?> f : futures) {
        if (!exceptions.isEmpty()) {
          break;
        }
        try {
          f.get();
        } catch (ExecutionException e) {
          LOG.error("Unexpected exec exception!  Should've been caught already.  (Bug?)", e);
          // Shouldn't happen, we already logged/caught any exceptions in the Runnable
        }
      }
    } catch (IOException e) {
      LOG.error("Cannot execute getTableStoreFilePathMap for " + tableName, e);
      exceptions.add(e);
    } finally {
      if (!exceptions.isEmpty()) {
        // Just throw the first exception as an indication something bad happened
        // Don't need to propagate all the exceptions, we already logged them all anyway
        Throwables.propagateIfInstanceOf(exceptions.firstElement(), IOException.class);
        throw Throwables.propagate(exceptions.firstElement());
      }
    }

    return finalResultMap;
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
   * @throws InterruptedException
   */
  public static Map<String, Path> getTableStoreFilePathMap(
    final FileSystem fs, final Path hbaseRootDir)
  throws IOException, InterruptedException {
    return getTableStoreFilePathMap(fs, hbaseRootDir, null, null, null);
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
   * @param sfFilter optional path filter to apply to store files
   * @param executor optional executor service to parallelize this operation
   * @param errors ErrorReporter instance or null
   * @return Map keyed by StoreFile name with a value of the full Path.
   * @throws IOException When scanning the directory fails.
   * @throws InterruptedException
   */
  public static Map<String, Path> getTableStoreFilePathMap(
    final FileSystem fs, final Path hbaseRootDir, PathFilter sfFilter,
    ExecutorService executor, ErrorReporter errors)
  throws IOException, InterruptedException {
    ConcurrentHashMap<String, Path> map = new ConcurrentHashMap<>(1024, 0.75f, 32);

    // if this method looks similar to 'getTableFragmentation' that is because
    // it was borrowed from it.

    // only include the directory paths to tables
    for (Path tableDir : FSUtils.getTableDirs(fs, hbaseRootDir)) {
      getTableStoreFilePathMap(map, fs, hbaseRootDir,
          FSUtils.getTableName(tableDir), sfFilter, executor, errors);
    }
    return map;
  }

  /**
   * Filters FileStatuses in an array and returns a list
   *
   * @param input   An array of FileStatuses
   * @param filter  A required filter to filter the array
   * @return        A list of FileStatuses
   */
  public static List<FileStatus> filterFileStatuses(FileStatus[] input,
      FileStatusFilter filter) {
    if (input == null) return null;
    return filterFileStatuses(Iterators.forArray(input), filter);
  }

  /**
   * Filters FileStatuses in an iterator and returns a list
   *
   * @param input   An iterator of FileStatuses
   * @param filter  A required filter to filter the array
   * @return        A list of FileStatuses
   */
  public static List<FileStatus> filterFileStatuses(Iterator<FileStatus> input,
      FileStatusFilter filter) {
    if (input == null) return null;
    ArrayList<FileStatus> results = new ArrayList<>();
    while (input.hasNext()) {
      FileStatus f = input.next();
      if (filter.accept(f)) {
        results.add(f);
      }
    }
    return results;
  }

  /**
   * Calls fs.listStatus() and treats FileNotFoundException as non-fatal
   * This accommodates differences between hadoop versions, where hadoop 1
   * does not throw a FileNotFoundException, and return an empty FileStatus[]
   * while Hadoop 2 will throw FileNotFoundException.
   *
   * @param fs file system
   * @param dir directory
   * @param filter file status filter
   * @return null if dir is empty or doesn't exist, otherwise FileStatus list
   */
  public static List<FileStatus> listStatusWithStatusFilter(final FileSystem fs,
      final Path dir, final FileStatusFilter filter) throws IOException {
    FileStatus [] status = null;
    try {
      status = fs.listStatus(dir);
    } catch (FileNotFoundException fnfe) {
      // if directory doesn't exist, return null
      if (LOG.isTraceEnabled()) {
        LOG.trace(dir + " doesn't exist");
      }
    }

    if (status == null || status.length < 1)  {
      return null;
    }

    if (filter == null) {
      return Arrays.asList(status);
    } else {
      List<FileStatus> status2 = filterFileStatuses(status, filter);
      if (status2 == null || status2.isEmpty()) {
        return null;
      } else {
        return status2;
      }
    }
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
    Map<String, Map<String, Float>> regionDegreeLocalityMapping = new ConcurrentHashMap<>();
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
        new ArrayBlockingQueue<>(statusList.length));
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

  /**
   * @param c
   * @return The DFSClient DFSHedgedReadMetrics instance or null if can't be found or not on hdfs.
   * @throws IOException
   */
  public static DFSHedgedReadMetrics getDFSHedgedReadMetrics(final Configuration c)
      throws IOException {
    if (!isHDFS(c)) return null;
    // getHedgedReadMetrics is package private. Get the DFSClient instance that is internal
    // to the DFS FS instance and make the method getHedgedReadMetrics accessible, then invoke it
    // to get the singleton instance of DFSHedgedReadMetrics shared by DFSClients.
    final String name = "getHedgedReadMetrics";
    DFSClient dfsclient = ((DistributedFileSystem)FileSystem.get(c)).getClient();
    Method m;
    try {
      m = dfsclient.getClass().getDeclaredMethod(name);
    } catch (NoSuchMethodException e) {
      LOG.warn("Failed find method " + name + " in dfsclient; no hedged read metrics: " +
          e.getMessage());
      return null;
    } catch (SecurityException e) {
      LOG.warn("Failed find method " + name + " in dfsclient; no hedged read metrics: " +
          e.getMessage());
      return null;
    }
    m.setAccessible(true);
    try {
      return (DFSHedgedReadMetrics)m.invoke(dfsclient);
    } catch (IllegalAccessException e) {
      LOG.warn("Failed invoking method " + name + " on dfsclient; no hedged read metrics: " +
          e.getMessage());
      return null;
    } catch (IllegalArgumentException e) {
      LOG.warn("Failed invoking method " + name + " on dfsclient; no hedged read metrics: " +
          e.getMessage());
      return null;
    } catch (InvocationTargetException e) {
      LOG.warn("Failed invoking method " + name + " on dfsclient; no hedged read metrics: " +
          e.getMessage());
      return null;
    }
  }

  public static List<Path> copyFilesParallel(FileSystem srcFS, Path src, FileSystem dstFS, Path dst,
      Configuration conf, int threads) throws IOException {
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    List<Future<Void>> futures = new ArrayList<>();
    List<Path> traversedPaths;
    try {
      traversedPaths = copyFiles(srcFS, src, dstFS, dst, conf, pool, futures);
      for (Future<Void> future : futures) {
        future.get();
      }
    } catch (ExecutionException | InterruptedException | IOException e) {
      throw new IOException("copy snapshot reference files failed", e);
    } finally {
      pool.shutdownNow();
    }
    return traversedPaths;
  }

  private static List<Path> copyFiles(FileSystem srcFS, Path src, FileSystem dstFS, Path dst,
      Configuration conf, ExecutorService pool, List<Future<Void>> futures) throws IOException {
    List<Path> traversedPaths = new ArrayList<>();
    traversedPaths.add(dst);
    FileStatus currentFileStatus = srcFS.getFileStatus(src);
    if (currentFileStatus.isDirectory()) {
      if (!dstFS.mkdirs(dst)) {
        throw new IOException("create dir failed: " + dst);
      }
      FileStatus[] subPaths = srcFS.listStatus(src);
      for (FileStatus subPath : subPaths) {
        traversedPaths.addAll(copyFiles(srcFS, subPath.getPath(), dstFS,
          new Path(dst, subPath.getPath().getName()), conf, pool, futures));
      }
    } else {
      Future<Void> future = pool.submit(() -> {
        FileUtil.copy(srcFS, src, dstFS, dst, false, false, conf);
        return null;
      });
      futures.add(future);
    }
    return traversedPaths;
  }
}
