/*
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
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.regionserver.wal.AbstractFSWAL;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.RecoverLeaseFSUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

/**
 * Base class of a WAL Provider that returns a single thread safe WAL that writes to Hadoop FS. By
 * default, this implementation picks a directory in Hadoop FS based on a combination of
 * <ul>
 * <li>the HBase root directory
 * <li>HConstants.HREGION_LOGDIR_NAME
 * <li>the given factory's factoryId (usually identifying the regionserver by host:port)
 * </ul>
 * It also uses the providerId to differentiate among files.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class AbstractFSWALProvider<T extends AbstractFSWAL<?>>
  extends AbstractWALProvider {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractFSWALProvider.class);

  /** Separate old log into different dir by regionserver name **/
  public static final String SEPARATE_OLDLOGDIR = "hbase.separate.oldlogdir.by.regionserver";
  public static final boolean DEFAULT_SEPARATE_OLDLOGDIR = false;

  public interface Initializer {
    /**
     * A method to initialize a WAL reader.
     * @param startPosition the start position you want to read from, -1 means start reading from
     *                      the first WAL entry. Notice that, the first entry is not started at
     *                      position as we have several headers, so typically you should not pass 0
     *                      here.
     */
    void init(FileSystem fs, Path path, Configuration c, long startPosition) throws IOException;
  }

  protected volatile T wal;

  /**
   * We use walCreateLock to prevent wal recreation in different threads, and also prevent getWALs
   * missing the newly created WAL, see HBASE-21503 for more details.
   */
  private final ReadWriteLock walCreateLock = new ReentrantReadWriteLock();

  /**
   * @param factory    factory that made us, identity used for FS layout. may not be null
   * @param conf       may not be null
   * @param providerId differentiate between providers from one factory, used for FS layout. may be
   *                   null
   */
  @Override
  protected void doInit(WALFactory factory, Configuration conf, String providerId)
    throws IOException {
    this.providerId = providerId;
    // get log prefix
    StringBuilder sb = new StringBuilder().append(factory.factoryId);
    if (providerId != null) {
      if (providerId.startsWith(WAL_FILE_NAME_DELIMITER)) {
        sb.append(providerId);
      } else {
        sb.append(WAL_FILE_NAME_DELIMITER).append(providerId);
      }
    }
    logPrefix = sb.toString();
    doInit(conf);
  }

  @Override
  protected List<WAL> getWALs0() {
    if (wal != null) {
      return Lists.newArrayList(wal);
    }
    walCreateLock.readLock().lock();
    try {
      if (wal == null) {
        return Collections.emptyList();
      } else {
        return Lists.newArrayList(wal);
      }
    } finally {
      walCreateLock.readLock().unlock();
    }
  }

  @Override
  protected T getWAL0(RegionInfo region) throws IOException {
    T walCopy = wal;
    if (walCopy != null) {
      return walCopy;
    }
    walCreateLock.writeLock().lock();
    try {
      walCopy = wal;
      if (walCopy != null) {
        return walCopy;
      }
      walCopy = createWAL();
      initWAL(walCopy);
      wal = walCopy;
      return walCopy;
    } finally {
      walCreateLock.writeLock().unlock();
    }
  }

  protected abstract T createWAL() throws IOException;

  protected abstract void doInit(Configuration conf) throws IOException;

  @Override
  protected void shutdown0() throws IOException {
    T log = this.wal;
    if (log != null) {
      log.shutdown();
    }
  }

  @Override
  protected void close0() throws IOException {
    T log = this.wal;
    if (log != null) {
      log.close();
    }
  }

  /**
   * iff the given WALFactory is using the DefaultWALProvider for meta and/or non-meta, count the
   * number of files (rolled and active). if either of them aren't, count 0 for that provider.
   */
  @Override
  protected long getNumLogFiles0() {
    T log = this.wal;
    return log == null ? 0 : log.getNumLogFiles();
  }

  /**
   * iff the given WALFactory is using the DefaultWALProvider for meta and/or non-meta, count the
   * size of files (only rolled). if either of them aren't, count 0 for that provider.
   */
  @Override
  protected long getLogFileSize0() {
    T log = this.wal;
    return log == null ? 0 : log.getLogFileSize();
  }

  @Override
  protected WAL createRemoteWAL(RegionInfo region, FileSystem remoteFs, Path remoteWALDir,
    String prefix, String suffix) throws IOException {
    // so we do not need to add this for a lot of test classes, for normal WALProvider, you should
    // implement this method to support sync replication.
    throw new UnsupportedOperationException();
  }

  /**
   * returns the number of rolled WAL files.
   */
  public static int getNumRolledLogFiles(WAL wal) {
    return ((AbstractFSWAL<?>) wal).getNumRolledLogFiles();
  }

  /**
   * returns the size of rolled WAL files.
   */
  public static long getLogFileSize(WAL wal) {
    return ((AbstractFSWAL<?>) wal).getLogFileSize();
  }

  /**
   * return the current filename from the current wal.
   */
  public static Path getCurrentFileName(final WAL wal) {
    return ((AbstractFSWAL<?>) wal).getCurrentFileName();
  }

  /**
   * request a log roll, but don't actually do it.
   */
  static void requestLogRoll(final WAL wal) {
    ((AbstractFSWAL<?>) wal).requestLogRoll();
  }

  // should be package private; more visible for use in AbstractFSWAL
  public static final String WAL_FILE_NAME_DELIMITER = ".";
  /** The hbase:meta region's WAL filename extension */
  public static final String META_WAL_PROVIDER_ID = ".meta";
  static final String DEFAULT_PROVIDER_ID = "default";

  // Implementation details that currently leak in tests or elsewhere follow
  /** File Extension used while splitting an WAL into regions (HBASE-2312) */
  public static final String SPLITTING_EXT = "-splitting";

  /**
   * Pattern used to validate a WAL file name see {@link #validateWALFilename(String)} for
   * description.
   */
  private static final Pattern WAL_FILE_NAME_PATTERN =
    Pattern.compile("(.+)\\.(\\d+)(\\.[0-9A-Za-z]+)?");

  /**
   * Define for when no timestamp found.
   */
  private static final long NO_TIMESTAMP = -1L;

  /**
   * It returns the file create timestamp (the 'FileNum') from the file name. For name format see
   * {@link #validateWALFilename(String)} public until remaining tests move to o.a.h.h.wal
   * @param wal must not be null
   * @return the file number that is part of the WAL file name
   */
  public static long extractFileNumFromWAL(final WAL wal) {
    final Path walPath = ((AbstractFSWAL<?>) wal).getCurrentFileName();
    if (walPath == null) {
      throw new IllegalArgumentException("The WAL path couldn't be null");
    }
    String name = walPath.getName();
    long timestamp = getTimestamp(name);
    if (timestamp == NO_TIMESTAMP) {
      throw new IllegalArgumentException(name + " is not a valid wal file name");
    }
    return timestamp;
  }

  /**
   * A WAL file name is of the format: &lt;wal-name&gt;{@link #WAL_FILE_NAME_DELIMITER}
   * &lt;file-creation-timestamp&gt;[.&lt;suffix&gt;]. provider-name is usually made up of a
   * server-name and a provider-id
   * @param filename name of the file to validate
   * @return <tt>true</tt> if the filename matches an WAL, <tt>false</tt> otherwise
   */
  public static boolean validateWALFilename(String filename) {
    return WAL_FILE_NAME_PATTERN.matcher(filename).matches();
  }

  /**
   * Split a WAL filename to get a start time. WALs usually have the time we start writing to them
   * with as part of their name, usually the suffix. Sometimes there will be an extra suffix as when
   * it is a WAL for the meta table. For example, WALs might look like this
   * <code>10.20.20.171%3A60020.1277499063250</code> where <code>1277499063250</code> is the
   * timestamp. Could also be a meta WAL which adds a '.meta' suffix or a synchronous replication
   * WAL which adds a '.syncrep' suffix. Check for these. File also may have no timestamp on it. For
   * example the recovered.edits files are WALs but are named in ascending order. Here is an
   * example: 0000000000000016310. Allow for this.
   * @param name Name of the WAL file.
   * @return Timestamp or {@link #NO_TIMESTAMP}.
   */
  public static long getTimestamp(String name) {
    Matcher matcher = WAL_FILE_NAME_PATTERN.matcher(name);
    return matcher.matches() ? Long.parseLong(matcher.group(2)) : NO_TIMESTAMP;
  }

  public static final Comparator<Path> TIMESTAMP_COMPARATOR =
    Comparator.<Path, Long> comparing(p -> AbstractFSWALProvider.getTimestamp(p.getName()))
      .thenComparing(Path::getName);

  /**
   * Construct the directory name for all WALs on a given server. Dir names currently look like this
   * for WALs: <code>hbase//WALs/kalashnikov.att.net,61634,1486865297088</code>.
   * @param serverName Server name formatted as described in {@link ServerName}
   * @return the relative WAL directory name, e.g. <code>.logs/1.example.org,60030,12345</code> if
   *         <code>serverName</code> passed is <code>1.example.org,60030,12345</code>
   */
  public static String getWALDirectoryName(String serverName) {
    StringBuilder dirName = new StringBuilder(HConstants.HREGION_LOGDIR_NAME);
    dirName.append(Path.SEPARATOR);
    dirName.append(getServerName(serverName));
    return dirName.toString();
  }

  /**
   * Construct the directory name for all old WALs on a given server. The default old WALs dir looks
   * like: <code>hbase/oldWALs</code>. If you config hbase.separate.oldlogdir.by.regionserver to
   * true, it looks like <code>hbase//oldWALs/kalashnikov.att.net,61634,1486865297088</code>.
   * @param serverName Server name formatted as described in {@link ServerName}
   * @return the relative WAL directory name
   */
  public static String getWALArchiveDirectoryName(Configuration conf, String serverName) {
    StringBuilder dirName = new StringBuilder(HConstants.HREGION_OLDLOGDIR_NAME);
    if (conf.getBoolean(SEPARATE_OLDLOGDIR, DEFAULT_SEPARATE_OLDLOGDIR)) {
      dirName.append(Path.SEPARATOR);
      dirName.append(getServerName(serverName));
    }
    return dirName.toString();
  }

  private static String getServerName(String serverName) {
    String address =
      ServerName.isFullServerName(serverName) ? ServerName.valueOf(serverName).getHostname() : null;
    // If ServerName is IPV6 address, then need to encode server address
    if (address != null && ServerName.isIpv6ServerName(address)) {
      serverName = ServerName.getEncodedServerName(serverName).getServerName();
    }
    return serverName;
  }

  /**
   * List all the old wal files for a dead region server.
   * <p/>
   * Initially added for supporting replication, where we need to get the wal files to replicate for
   * a dead region server.
   */
  public static List<Path> getArchivedWALFiles(Configuration conf, ServerName serverName,
    String logPrefix) throws IOException {
    Path walRootDir = CommonFSUtils.getWALRootDir(conf);
    FileSystem fs = walRootDir.getFileSystem(conf);
    List<Path> archivedWalFiles = new ArrayList<>();
    // list both the root old wal dir and the separate old wal dir, so we will not miss any files if
    // the SEPARATE_OLDLOGDIR config is changed
    Path oldWalDir = new Path(walRootDir, HConstants.HREGION_OLDLOGDIR_NAME);
    try {
      for (FileStatus status : fs.listStatus(oldWalDir, p -> p.getName().startsWith(logPrefix))) {
        if (status.isFile()) {
          archivedWalFiles.add(status.getPath());
        }
      }
    } catch (FileNotFoundException e) {
      LOG.info("Old WAL dir {} not exists", oldWalDir);
      return Collections.emptyList();
    }
    Path separatedOldWalDir = new Path(oldWalDir, serverName.toString());
    try {
      for (FileStatus status : fs.listStatus(separatedOldWalDir,
        p -> p.getName().startsWith(logPrefix))) {
        if (status.isFile()) {
          archivedWalFiles.add(status.getPath());
        }
      }
    } catch (FileNotFoundException e) {
      LOG.info("Seprated old WAL dir {} not exists", separatedOldWalDir);
    }
    return archivedWalFiles;
  }

  /**
   * List all the wal files for a logPrefix.
   */
  public static List<Path> getWALFiles(Configuration c, ServerName serverName) throws IOException {
    Path walRoot = new Path(CommonFSUtils.getWALRootDir(c), HConstants.HREGION_LOGDIR_NAME);
    FileSystem fs = walRoot.getFileSystem(c);
    List<Path> walFiles = new ArrayList<>();
    Path walDir = new Path(walRoot, serverName.toString());
    try {
      for (FileStatus status : fs.listStatus(walDir)) {
        if (status.isFile()) {
          walFiles.add(status.getPath());
        }
      }
    } catch (FileNotFoundException e) {
      LOG.info("WAL dir {} not exists", walDir);
    }
    return walFiles;
  }

  /**
   * Pulls a ServerName out of a Path generated according to our layout rules. In the below layouts,
   * this method ignores the format of the logfile component. Current format: [base directory for
   * hbase]/hbase/.logs/ServerName/logfile or [base directory for
   * hbase]/hbase/.logs/ServerName-splitting/logfile Expected to work for individual log files and
   * server-specific directories.
   * @return null if it's not a log file. Returns the ServerName of the region server that created
   *         this log file otherwise.
   */
  public static ServerName getServerNameFromWALDirectoryName(Configuration conf, String path)
    throws IOException {
    if (path == null || path.length() <= HConstants.HREGION_LOGDIR_NAME.length()) {
      return null;
    }

    if (conf == null) {
      throw new IllegalArgumentException("parameter conf must be set");
    }

    final String rootDir = conf.get(HConstants.HBASE_DIR);
    if (rootDir == null || rootDir.isEmpty()) {
      throw new IllegalArgumentException(HConstants.HBASE_DIR + " key not found in conf.");
    }

    final StringBuilder startPathSB = new StringBuilder(rootDir);
    if (!rootDir.endsWith("/")) {
      startPathSB.append('/');
    }
    startPathSB.append(HConstants.HREGION_LOGDIR_NAME);
    if (!HConstants.HREGION_LOGDIR_NAME.endsWith("/")) {
      startPathSB.append('/');
    }
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
    return getServerNameFromWALDirectoryName(p);
  }

  /**
   * This function returns region server name from a log file name which is in one of the following
   * formats:
   * <ul>
   * <li>hdfs://&lt;name node&gt;/hbase/.logs/&lt;server name&gt;-splitting/...</li>
   * <li>hdfs://&lt;name node&gt;/hbase/.logs/&lt;server name&gt;/...</li>
   * </ul>
   * @return null if the passed in logFile isn't a valid WAL file path
   */
  public static ServerName getServerNameFromWALDirectoryName(Path logFile) {
    String logDirName = logFile.getParent().getName();
    // We were passed the directory and not a file in it.
    if (logDirName.equals(HConstants.HREGION_LOGDIR_NAME)) {
      logDirName = logFile.getName();
    }
    ServerName serverName = null;
    if (logDirName.endsWith(SPLITTING_EXT)) {
      logDirName = logDirName.substring(0, logDirName.length() - SPLITTING_EXT.length());
    }
    try {
      serverName = ServerName.parseServerName(logDirName);
    } catch (IllegalArgumentException | IllegalStateException ex) {
      serverName = null;
      LOG.warn("Cannot parse a server name from path={}", logFile, ex);
    }
    if (serverName != null && serverName.getStartCode() < 0) {
      LOG.warn("Invalid log file path={}, start code {} is less than 0", logFile,
        serverName.getStartCode());
      serverName = null;
    }
    return serverName;
  }

  public static boolean isMetaFile(Path p) {
    return isMetaFile(p.getName());
  }

  /** Returns True if String ends in {@link #META_WAL_PROVIDER_ID} */
  public static boolean isMetaFile(String p) {
    return p != null && p.endsWith(META_WAL_PROVIDER_ID);
  }

  /**
   * Comparator used to compare WAL files together based on their start time. Just compares start
   * times and nothing else.
   */
  public static class WALStartTimeComparator implements Comparator<Path> {
    @Override
    public int compare(Path o1, Path o2) {
      return Long.compare(getTS(o1), getTS(o2));
    }

    /**
     * Split a path to get the start time For example: 10.20.20.171%3A60020.1277499063250 Could also
     * be a meta WAL which adds a '.meta' suffix or a synchronous replication WAL which adds a
     * '.syncrep' suffix. Check.
     * @param p path to split
     * @return start time
     */
    public static long getTS(Path p) {
      return getTimestamp(p.getName());
    }
  }

  public static boolean isArchivedLogFile(Path p) {
    String oldLog = Path.SEPARATOR + HConstants.HREGION_OLDLOGDIR_NAME + Path.SEPARATOR;
    return p.toString().contains(oldLog);
  }

  /**
   * Find the archived WAL file path if it is not able to locate in WALs dir.
   * @param path - active WAL file path
   * @param conf - configuration
   * @return archived path if exists, null - otherwise
   * @throws IOException exception
   */
  public static Path findArchivedLog(Path path, Configuration conf) throws IOException {
    // If the path contains oldWALs keyword then exit early.
    if (path.toString().contains(HConstants.HREGION_OLDLOGDIR_NAME)) {
      return null;
    }
    Path walRootDir = CommonFSUtils.getWALRootDir(conf);
    FileSystem fs = path.getFileSystem(conf);
    // Try finding the log in old dir
    Path oldLogDir = new Path(walRootDir, HConstants.HREGION_OLDLOGDIR_NAME);
    Path archivedLogLocation = new Path(oldLogDir, path.getName());
    if (fs.exists(archivedLogLocation)) {
      LOG.info("Log " + path + " was moved to " + archivedLogLocation);
      return archivedLogLocation;
    }

    ServerName serverName = getServerNameFromWALDirectoryName(path);
    if (serverName == null) {
      LOG.warn("Can not extract server name from path {}, "
        + "give up searching the separated old log dir", path);
      return null;
    }
    // Try finding the log in separate old log dir
    oldLogDir = new Path(walRootDir, new StringBuilder(HConstants.HREGION_OLDLOGDIR_NAME)
      .append(Path.SEPARATOR).append(serverName.getServerName()).toString());
    archivedLogLocation = new Path(oldLogDir, path.getName());
    if (fs.exists(archivedLogLocation)) {
      LOG.info("Log " + path + " was moved to " + archivedLogLocation);
      return archivedLogLocation;
    }
    LOG.error("Couldn't locate log: " + path);
    return null;
  }

  // For HBASE-15019
  public static void recoverLease(Configuration conf, Path path) {
    try {
      final FileSystem dfs = CommonFSUtils.getCurrentFileSystem(conf);
      RecoverLeaseFSUtils.recoverFileLease(dfs, path, conf, new CancelableProgressable() {
        @Override
        public boolean progress() {
          LOG.debug("Still trying to recover WAL lease: " + path);
          return true;
        }
      });
    } catch (IOException e) {
      LOG.warn("unable to recover lease for WAL: " + path, e);
    }
  }

  @Override
  public void addWALActionsListener(WALActionsListener listener) {
    listeners.add(listener);
  }

  private static String getWALNameGroupFromWALName(String name, int group) {
    Matcher matcher = WAL_FILE_NAME_PATTERN.matcher(name);
    if (matcher.matches()) {
      return matcher.group(group);
    } else {
      throw new IllegalArgumentException(name + " is not a valid wal file name");
    }
  }

  /**
   * Get prefix of the log from its name, assuming WAL name in format of
   * log_prefix.filenumber.log_suffix
   * @param name Name of the WAL to parse
   * @return prefix of the log
   * @throws IllegalArgumentException if the name passed in is not a valid wal file name
   * @see AbstractFSWAL#getCurrentFileName()
   */
  public static String getWALPrefixFromWALName(String name) {
    return getWALNameGroupFromWALName(name, 1);
  }

  private static final Pattern SERVER_NAME_PATTERN = Pattern.compile("^[^"
    + ServerName.SERVERNAME_SEPARATOR + "]+" + ServerName.SERVERNAME_SEPARATOR
    + Addressing.VALID_PORT_REGEX + ServerName.SERVERNAME_SEPARATOR + Addressing.VALID_PORT_REGEX);

  /**
   * Parse the server name from wal prefix. A wal's name is always started with a server name in non
   * test code.
   * @throws IllegalArgumentException if the name passed in is not started with a server name
   * @return the server name
   */
  public static ServerName parseServerNameFromWALName(String name) {
    String decoded;
    try {
      decoded = URLDecoder.decode(name, StandardCharsets.UTF_8.name());
    } catch (UnsupportedEncodingException e) {
      throw new AssertionError("should never happen", e);
    }
    Matcher matcher = SERVER_NAME_PATTERN.matcher(decoded);
    if (matcher.find()) {
      return ServerName.valueOf(matcher.group());
    } else {
      throw new IllegalArgumentException(name + " is not started with a server name");
    }
  }
}
