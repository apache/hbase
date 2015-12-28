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
package org.apache.hadoop.hbase.wal;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.FSUtils;

// imports for things that haven't moved from regionserver.wal yet.
import org.apache.hadoop.hbase.regionserver.wal.FSHLog;
import org.apache.hadoop.hbase.regionserver.wal.ProtobufLogWriter;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;

/**
 * A WAL Provider that returns a single thread safe WAL that writes to HDFS.
 * By default, this implementation picks a directory in HDFS based on a combination of
 * <ul>
 *   <li>the HBase root directory
 *   <li>HConstants.HREGION_LOGDIR_NAME
 *   <li>the given factory's factoryId (usually identifying the regionserver by host:port)
 * </ul>
 * It also uses the providerId to diffentiate among files.
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DefaultWALProvider implements WALProvider {
  private static final Log LOG = LogFactory.getLog(DefaultWALProvider.class);

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

  // Only public so classes back in regionserver.wal can access
  public interface Writer extends WALProvider.Writer {
    void init(FileSystem fs, Path path, Configuration c, boolean overwritable) throws IOException;
  }

  protected volatile FSHLog log = null;
  private WALFactory factory = null;
  private Configuration conf = null;
  private List<WALActionsListener> listeners = null;
  private String providerId = null;
  private AtomicBoolean initialized = new AtomicBoolean(false);
  // for default wal provider, logPrefix won't change
  private String logPrefix = null;

  /**
   * we synchronized on walCreateLock to prevent wal recreation in different threads
   */
  private final Object walCreateLock = new Object();

  /**
   * @param factory factory that made us, identity used for FS layout. may not be null
   * @param conf may not be null
   * @param listeners may be null
   * @param providerId differentiate between providers from one facotry, used for FS layout. may be
   *                   null
   */
  @Override
  public void init(final WALFactory factory, final Configuration conf,
      final List<WALActionsListener> listeners, String providerId) throws IOException {
    if (!initialized.compareAndSet(false, true)) {
      throw new IllegalStateException("WALProvider.init should only be called once.");
    }
    this.factory = factory;
    this.conf = conf;
    this.listeners = listeners;
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
  }

  @Override
  public WAL getWAL(final byte[] identifier, byte[] namespace) throws IOException {
    if (log == null) {
      // only lock when need to create wal, and need to lock since
      // creating hlog on fs is time consuming
      synchronized (walCreateLock) {
        if (log == null) {
          log = new FSHLog(FileSystem.get(conf), FSUtils.getRootDir(conf),
              getWALDirectoryName(factory.factoryId), HConstants.HREGION_OLDLOGDIR_NAME, conf,
              listeners, true, logPrefix,
              META_WAL_PROVIDER_ID.equals(providerId) ? META_WAL_PROVIDER_ID : null);
        }
      }
    }
    return log;
  }

  @Override
  public void close() throws IOException {
    if (log != null) log.close();
  }

  @Override
  public void shutdown() throws IOException {
    if (log != null) log.shutdown();
  }

  // should be package private; more visible for use in FSHLog
  public static final String WAL_FILE_NAME_DELIMITER = ".";
  /** The hbase:meta region's WAL filename extension */
  @VisibleForTesting
  public static final String META_WAL_PROVIDER_ID = ".meta";
  static final String DEFAULT_PROVIDER_ID = "default";

  // Implementation details that currently leak in tests or elsewhere follow
  /** File Extension used while splitting an WAL into regions (HBASE-2312) */
  public static final String SPLITTING_EXT = "-splitting";

  /**
   * iff the given WALFactory is using the DefaultWALProvider for meta and/or non-meta,
   * count the number of files (rolled and active). if either of them aren't, count 0
   * for that provider.
   */
  @Override
  public long getNumLogFiles() {
    return log == null ? 0 : this.log.getNumLogFiles();
  }

  /**
   * iff the given WALFactory is using the DefaultWALProvider for meta and/or non-meta,
   * count the size of files (rolled and active). if either of them aren't, count 0
   * for that provider.
   */
  @Override
  public long getLogFileSize() {
    return log == null ? 0 : this.log.getLogFileSize();
  }

  /**
   * returns the number of rolled WAL files.
   */
  @VisibleForTesting
  public static int getNumRolledLogFiles(WAL wal) {
    return ((FSHLog)wal).getNumRolledLogFiles();
  }

  /**
   * return the current filename from the current wal.
   */
  @VisibleForTesting
  public static Path getCurrentFileName(final WAL wal) {
    return ((FSHLog)wal).getCurrentFileName();
  }

  /**
   * request a log roll, but don't actually do it.
   */
  @VisibleForTesting
  static void requestLogRoll(final WAL wal) {
    ((FSHLog)wal).requestLogRoll();
  }

  /**
   * It returns the file create timestamp from the file name.
   * For name format see {@link #validateWALFilename(String)}
   * public until remaining tests move to o.a.h.h.wal
   * @param wal must not be null
   * @return the file number that is part of the WAL file name
   */
  @VisibleForTesting
  public static long extractFileNumFromWAL(final WAL wal) {
    final Path walName = ((FSHLog)wal).getCurrentFileName();
    if (walName == null) {
      throw new IllegalArgumentException("The WAL path couldn't be null");
    }
    final String[] walPathStrs = walName.toString().split("\\" + WAL_FILE_NAME_DELIMITER);
    return Long.parseLong(walPathStrs[walPathStrs.length - (isMetaFile(walName) ? 2:1)]);
  }

  /**
   * Pattern used to validate a WAL file name
   * see {@link #validateWALFilename(String)} for description.
   */
  private static final Pattern pattern = Pattern.compile(".*\\.\\d*("+META_WAL_PROVIDER_ID+")*");

  /**
   * A WAL file name is of the format:
   * &lt;wal-name&gt;{@link #WAL_FILE_NAME_DELIMITER}&lt;file-creation-timestamp&gt;[.meta].
   *
   * provider-name is usually made up of a server-name and a provider-id
   *
   * @param filename name of the file to validate
   * @return <tt>true</tt> if the filename matches an WAL, <tt>false</tt>
   *         otherwise
   */
  public static boolean validateWALFilename(String filename) {
    return pattern.matcher(filename).matches();
  }

  /**
   * Construct the directory name for all WALs on a given server.
   *
   * @param serverName
   *          Server name formatted as described in {@link ServerName}
   * @return the relative WAL directory name, e.g.
   *         <code>.logs/1.example.org,60030,12345</code> if
   *         <code>serverName</code> passed is
   *         <code>1.example.org,60030,12345</code>
   */
  public static String getWALDirectoryName(final String serverName) {
    StringBuilder dirName = new StringBuilder(HConstants.HREGION_LOGDIR_NAME);
    dirName.append("/");
    dirName.append(serverName);
    return dirName.toString();
  }

  /**
   * Pulls a ServerName out of a Path generated according to our layout rules.
   *
   * In the below layouts, this method ignores the format of the logfile component.
   *
   * Current format:
   *
   * [base directory for hbase]/hbase/.logs/ServerName/logfile
   *      or
   * [base directory for hbase]/hbase/.logs/ServerName-splitting/logfile
   *
   * Expected to work for individual log files and server-specific directories.
   *
   * @return null if it's not a log file. Returns the ServerName of the region
   *         server that created this log file otherwise.
   */
  public static ServerName getServerNameFromWALDirectoryName(Configuration conf, String path)
      throws IOException {
    if (path == null
        || path.length() <= HConstants.HREGION_LOGDIR_NAME.length()) {
      return null;
    }

    if (conf == null) {
      throw new IllegalArgumentException("parameter conf must be set");
    }

    final String rootDir = conf.get(HConstants.HBASE_DIR);
    if (rootDir == null || rootDir.isEmpty()) {
      throw new IllegalArgumentException(HConstants.HBASE_DIR
          + " key not found in conf.");
    }

    final StringBuilder startPathSB = new StringBuilder(rootDir);
    if (!rootDir.endsWith("/"))
      startPathSB.append('/');
    startPathSB.append(HConstants.HREGION_LOGDIR_NAME);
    if (!HConstants.HREGION_LOGDIR_NAME.endsWith("/"))
      startPathSB.append('/');
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
   *   <li>hdfs://&lt;name node&gt;/hbase/.logs/&lt;server name&gt;-splitting/...</li>
   *   <li>hdfs://&lt;name node&gt;/hbase/.logs/&lt;server name&gt;/...</li>
   * </ul>
   * @param logFile
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
    } catch (IllegalArgumentException ex) {
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

  /**
   * public because of FSHLog. Should be package-private
   */
  public static Writer createWriter(final Configuration conf, final FileSystem fs, final Path path,
      final boolean overwritable)
      throws IOException {
    // Configuration already does caching for the Class lookup.
    Class<? extends Writer> logWriterClass = conf.getClass("hbase.regionserver.hlog.writer.impl",
        ProtobufLogWriter.class, Writer.class);
    try {
      Writer writer = logWriterClass.newInstance();
      writer.init(fs, path, conf, overwritable);
      return writer;
    } catch (Exception e) {
      LOG.debug("Error instantiating log writer.", e);
      throw new IOException("cannot get log writer", e);
    }
  }

  /**
   * Get prefix of the log from its name, assuming WAL name in format of
   * log_prefix.filenumber.log_suffix @see {@link FSHLog#getCurrentFileName()}
   * @param name Name of the WAL to parse
   * @return prefix of the log
   */
  public static String getWALPrefixFromWALName(String name) {
    int endIndex = name.replaceAll(META_WAL_PROVIDER_ID, "").lastIndexOf(".");
    return name.substring(0, endIndex);
  }

}
