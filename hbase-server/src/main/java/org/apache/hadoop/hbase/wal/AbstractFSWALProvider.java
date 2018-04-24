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
package org.apache.hadoop.hbase.wal;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.regionserver.wal.AbstractFSWAL;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.LeaseNotRecoveredException;
import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;

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
public abstract class AbstractFSWALProvider<T extends AbstractFSWAL<?>> implements WALProvider {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractFSWALProvider.class);

  /** Separate old log into different dir by regionserver name **/
  public static final String SEPARATE_OLDLOGDIR = "hbase.separate.oldlogdir.by.regionserver";
  public static final boolean DEFAULT_SEPARATE_OLDLOGDIR = false;

  // Only public so classes back in regionserver.wal can access
  public interface Reader extends WAL.Reader {
    /**
     * @param fs File system.
     * @param path Path.
     * @param c Configuration.
     * @param s Input stream that may have been pre-opened by the caller; may be null.
     */
    void init(FileSystem fs, Path path, Configuration c, FSDataInputStream s) throws IOException;
  }

  protected volatile T wal;
  protected WALFactory factory;
  protected Configuration conf;
  protected List<WALActionsListener> listeners = new ArrayList<>();
  protected String providerId;
  protected AtomicBoolean initialized = new AtomicBoolean(false);
  // for default wal provider, logPrefix won't change
  protected String logPrefix;

  /**
   * we synchronized on walCreateLock to prevent wal recreation in different threads
   */
  private final Object walCreateLock = new Object();

  /**
   * @param factory factory that made us, identity used for FS layout. may not be null
   * @param conf may not be null
   * @param providerId differentiate between providers from one factory, used for FS layout. may be
   *          null
   */
  @Override
  public void init(WALFactory factory, Configuration conf, String providerId) throws IOException {
    if (!initialized.compareAndSet(false, true)) {
      throw new IllegalStateException("WALProvider.init should only be called once.");
    }
    this.factory = factory;
    this.conf = conf;
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
  public List<WAL> getWALs() {
    if (wal == null) {
      return Collections.emptyList();
    }
    List<WAL> wals = new ArrayList<>(1);
    wals.add(wal);
    return wals;
  }

  @Override
  public T getWAL(RegionInfo region) throws IOException {
    T walCopy = wal;
    if (walCopy == null) {
      // only lock when need to create wal, and need to lock since
      // creating hlog on fs is time consuming
      synchronized (walCreateLock) {
        walCopy = wal;
        if (walCopy == null) {
          walCopy = createWAL();
          boolean succ = false;
          try {
            walCopy.init();
            succ = true;
          } finally {
            if (!succ) {
              walCopy.close();
            }
          }
          wal = walCopy;
        }
      }
    }
    return walCopy;
  }

  protected abstract T createWAL() throws IOException;

  protected abstract void doInit(Configuration conf) throws IOException;

  @Override
  public void shutdown() throws IOException {
    T log = this.wal;
    if (log != null) {
      log.shutdown();
    }
  }

  @Override
  public void close() throws IOException {
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
  public long getNumLogFiles() {
    T log = this.wal;
    return log == null ? 0 : log.getNumLogFiles();
  }

  /**
   * iff the given WALFactory is using the DefaultWALProvider for meta and/or non-meta, count the
   * size of files (only rolled). if either of them aren't, count 0 for that provider.
   */
  @Override
  public long getLogFileSize() {
    T log = this.wal;
    return log == null ? 0 : log.getLogFileSize();
  }

  /**
   * returns the number of rolled WAL files.
   */
  @VisibleForTesting
  public static int getNumRolledLogFiles(WAL wal) {
    return ((AbstractFSWAL<?>) wal).getNumRolledLogFiles();
  }

  /**
   * returns the size of rolled WAL files.
   */
  @VisibleForTesting
  public static long getLogFileSize(WAL wal) {
    return ((AbstractFSWAL<?>) wal).getLogFileSize();
  }

  /**
   * return the current filename from the current wal.
   */
  @VisibleForTesting
  public static Path getCurrentFileName(final WAL wal) {
    return ((AbstractFSWAL<?>) wal).getCurrentFileName();
  }

  /**
   * request a log roll, but don't actually do it.
   */
  @VisibleForTesting
  static void requestLogRoll(final WAL wal) {
    ((AbstractFSWAL<?>) wal).requestLogRoll();
  }

  // should be package private; more visible for use in AbstractFSWAL
  public static final String WAL_FILE_NAME_DELIMITER = ".";
  /** The hbase:meta region's WAL filename extension */
  @VisibleForTesting
  public static final String META_WAL_PROVIDER_ID = ".meta";
  static final String DEFAULT_PROVIDER_ID = "default";

  // Implementation details that currently leak in tests or elsewhere follow
  /** File Extension used while splitting an WAL into regions (HBASE-2312) */
  public static final String SPLITTING_EXT = "-splitting";

  /**
   * It returns the file create timestamp from the file name. For name format see
   * {@link #validateWALFilename(String)} public until remaining tests move to o.a.h.h.wal
   * @param wal must not be null
   * @return the file number that is part of the WAL file name
   */
  @VisibleForTesting
  public static long extractFileNumFromWAL(final WAL wal) {
    final Path walName = ((AbstractFSWAL<?>) wal).getCurrentFileName();
    if (walName == null) {
      throw new IllegalArgumentException("The WAL path couldn't be null");
    }
    Matcher matcher = WAL_FILE_NAME_PATTERN.matcher(walName.getName());
    if (matcher.matches()) {
      return Long.parseLong(matcher.group(2));
    } else {
      throw new IllegalArgumentException(walName.getName() + " is not a valid wal file name");
    }
  }

  /**
   * Pattern used to validate a WAL file name see {@link #validateWALFilename(String)} for
   * description.
   */
  private static final Pattern WAL_FILE_NAME_PATTERN =
    Pattern.compile("(.+)\\.(\\d+)(\\.[0-9A-Za-z]+)?");

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
   * Construct the directory name for all WALs on a given server. Dir names currently look like this
   * for WALs: <code>hbase//WALs/kalashnikov.att.net,61634,1486865297088</code>.
   * @param serverName Server name formatted as described in {@link ServerName}
   * @return the relative WAL directory name, e.g. <code>.logs/1.example.org,60030,12345</code> if
   *         <code>serverName</code> passed is <code>1.example.org,60030,12345</code>
   */
  public static String getWALDirectoryName(final String serverName) {
    StringBuilder dirName = new StringBuilder(HConstants.HREGION_LOGDIR_NAME);
    dirName.append("/");
    dirName.append(serverName);
    return dirName.toString();
  }

  /**
   * Construct the directory name for all old WALs on a given server. The default old WALs dir looks
   * like: <code>hbase/oldWALs</code>. If you config hbase.separate.oldlogdir.by.regionserver to
   * true, it looks like <code>hbase//oldWALs/kalashnikov.att.net,61634,1486865297088</code>.
   * @param conf
   * @param serverName Server name formatted as described in {@link ServerName}
   * @return the relative WAL directory name
   */
  public static String getWALArchiveDirectoryName(Configuration conf, final String serverName) {
    StringBuilder dirName = new StringBuilder(HConstants.HREGION_OLDLOGDIR_NAME);
    if (conf.getBoolean(SEPARATE_OLDLOGDIR, DEFAULT_SEPARATE_OLDLOGDIR)) {
      dirName.append(Path.SEPARATOR);
      dirName.append(serverName);
    }
    return dirName.toString();
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
      LOG.warn("Cannot parse a server name from path=" + logFile + "; " + ex.getMessage());
    }
    if (serverName != null && serverName.getStartcode() < 0) {
      LOG.warn("Invalid log file path=" + logFile);
      serverName = null;
    }
    return serverName;
  }

  public static boolean isMetaFile(Path p) {
    return isMetaFile(p.getName());
  }

  public static boolean isMetaFile(String p) {
    if (p != null && p.endsWith(META_WAL_PROVIDER_ID)) {
      return true;
    }
    return false;
  }

  public static boolean isArchivedLogFile(Path p) {
    String oldLog = Path.SEPARATOR + HConstants.HREGION_OLDLOGDIR_NAME + Path.SEPARATOR;
    return p.toString().contains(oldLog);
  }

  /**
   * Get the archived WAL file path
   * @param path - active WAL file path
   * @param conf - configuration
   * @return archived path if exists, path - otherwise
   * @throws IOException exception
   */
  public static Path getArchivedLogPath(Path path, Configuration conf) throws IOException {
    Path rootDir = FSUtils.getRootDir(conf);
    Path oldLogDir = new Path(rootDir, HConstants.HREGION_OLDLOGDIR_NAME);
    if (conf.getBoolean(SEPARATE_OLDLOGDIR, DEFAULT_SEPARATE_OLDLOGDIR)) {
      ServerName serverName = getServerNameFromWALDirectoryName(path);
      if (serverName == null) {
        LOG.error("Couldn't locate log: " + path);
        return path;
      }
      oldLogDir = new Path(oldLogDir, serverName.getServerName());
    }
    Path archivedLogLocation = new Path(oldLogDir, path.getName());
    final FileSystem fs = FSUtils.getCurrentFileSystem(conf);

    if (fs.exists(archivedLogLocation)) {
      LOG.info("Log " + path + " was moved to " + archivedLogLocation);
      return archivedLogLocation;
    } else {
      LOG.error("Couldn't locate log: " + path);
      return path;
    }
  }

  /**
   * Opens WAL reader with retries and additional exception handling
   * @param path path to WAL file
   * @param conf configuration
   * @return WAL Reader instance
   * @throws IOException
   */
  public static org.apache.hadoop.hbase.wal.WAL.Reader openReader(Path path, Configuration conf)
      throws IOException

  {
    long retryInterval = 2000; // 2 sec
    int maxAttempts = 30;
    int attempt = 0;
    Exception ee = null;
    org.apache.hadoop.hbase.wal.WAL.Reader reader = null;
    while (reader == null && attempt++ < maxAttempts) {
      try {
        // Detect if this is a new file, if so get a new reader else
        // reset the current reader so that we see the new data
        reader = WALFactory.createReader(path.getFileSystem(conf), path, conf);
        return reader;
      } catch (FileNotFoundException fnfe) {
        // If the log was archived, continue reading from there
        Path archivedLog = AbstractFSWALProvider.getArchivedLogPath(path, conf);
        if (!Objects.equals(path, archivedLog)) {
          return openReader(archivedLog, conf);
        } else {
          throw fnfe;
        }
      } catch (LeaseNotRecoveredException lnre) {
        // HBASE-15019 the WAL was not closed due to some hiccup.
        LOG.warn("Try to recover the WAL lease " + path, lnre);
        recoverLease(conf, path);
        reader = null;
        ee = lnre;
      } catch (NullPointerException npe) {
        // Workaround for race condition in HDFS-4380
        // which throws a NPE if we open a file before any data node has the most recent block
        // Just sleep and retry. Will require re-reading compressed WALs for compressionContext.
        LOG.warn("Got NPE opening reader, will retry.");
        reader = null;
        ee = npe;
      }
      if (reader == null) {
        // sleep before next attempt
        try {
          Thread.sleep(retryInterval);
        } catch (InterruptedException e) {
        }
      }
    }
    throw new IOException("Could not open reader", ee);
  }

  // For HBASE-15019
  private static void recoverLease(final Configuration conf, final Path path) {
    try {
      final FileSystem dfs = FSUtils.getCurrentFileSystem(conf);
      FSUtils fsUtils = FSUtils.getInstance(dfs, conf);
      fsUtils.recoverFileLease(dfs, path, conf, new CancelableProgressable() {
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

  /**
   * Get prefix of the log from its name, assuming WAL name in format of
   * log_prefix.filenumber.log_suffix
   * @param name Name of the WAL to parse
   * @return prefix of the log
   * @throws IllegalArgumentException if the name passed in is not a valid wal file name
   * @see AbstractFSWAL#getCurrentFileName()
   */
  public static String getWALPrefixFromWALName(String name) {
    Matcher matcher = WAL_FILE_NAME_PATTERN.matcher(name);
    if (matcher.matches()) {
      return matcher.group(1);
    } else {
      throw new IllegalArgumentException(name + " is not a valid wal file name");
    }
  }
}
