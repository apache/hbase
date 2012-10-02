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

package org.apache.hadoop.hbase.regionserver.wal;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Reader;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Writer;
import org.apache.hadoop.hbase.util.FSUtils;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;

public class HLogUtil {
  static final Log LOG = LogFactory.getLog(HLogUtil.class);

  static byte[] COMPLETE_CACHE_FLUSH;
  static {
    try {
      COMPLETE_CACHE_FLUSH = "HBASE::CACHEFLUSH"
          .getBytes(HConstants.UTF8_ENCODING);
    } catch (UnsupportedEncodingException e) {
      assert (false);
    }
  }

  /**
   * @param family
   * @return true if the column is a meta column
   */
  public static boolean isMetaFamily(byte[] family) {
    return Bytes.equals(HLog.METAFAMILY, family);
  }

  @SuppressWarnings("unchecked")
  public static Class<? extends HLogKey> getKeyClass(Configuration conf) {
    return (Class<? extends HLogKey>) conf.getClass(
        "hbase.regionserver.hlog.keyclass", HLogKey.class);
  }

  public static HLogKey newKey(Configuration conf) throws IOException {
    Class<? extends HLogKey> keyClass = getKeyClass(conf);
    try {
      return keyClass.newInstance();
    } catch (InstantiationException e) {
      throw new IOException("cannot create hlog key");
    } catch (IllegalAccessException e) {
      throw new IOException("cannot create hlog key");
    }
  }

  /**
   * Pattern used to validate a HLog file name
   */
  private static final Pattern pattern = Pattern.compile(".*\\.\\d*");

  /**
   * @param filename
   *          name of the file to validate
   * @return <tt>true</tt> if the filename matches an HLog, <tt>false</tt>
   *         otherwise
   */
  public static boolean validateHLogFilename(String filename) {
    return pattern.matcher(filename).matches();
  }

  /*
   * Get a reader for the WAL.
   * 
   * @param fs
   * 
   * @param path
   * 
   * @param conf
   * 
   * @return A WAL reader. Close when done with it.
   * 
   * @throws IOException
   * 
   * public static HLog.Reader getReader(final FileSystem fs, final Path path,
   * Configuration conf) throws IOException { try {
   * 
   * if (logReaderClass == null) {
   * 
   * logReaderClass = conf.getClass("hbase.regionserver.hlog.reader.impl",
   * SequenceFileLogReader.class, Reader.class); }
   * 
   * 
   * HLog.Reader reader = logReaderClass.newInstance(); reader.init(fs, path,
   * conf); return reader; } catch (IOException e) { throw e; } catch (Exception
   * e) { throw new IOException("Cannot get log reader", e); } }
   * 
   * * Get a writer for the WAL.
   * 
   * @param path
   * 
   * @param conf
   * 
   * @return A WAL writer. Close when done with it.
   * 
   * @throws IOException
   * 
   * public static HLog.Writer createWriter(final FileSystem fs, final Path
   * path, Configuration conf) throws IOException { try { if (logWriterClass ==
   * null) { logWriterClass =
   * conf.getClass("hbase.regionserver.hlog.writer.impl",
   * SequenceFileLogWriter.class, Writer.class); } FSHLog.Writer writer =
   * (FSHLog.Writer) logWriterClass.newInstance(); writer.init(fs, path, conf);
   * return writer; } catch (Exception e) { throw new
   * IOException("cannot get log writer", e); } }
   */

  /**
   * Construct the HLog directory name
   * 
   * @param serverName
   *          Server name formatted as described in {@link ServerName}
   * @return the relative HLog directory name, e.g.
   *         <code>.logs/1.example.org,60030,12345</code> if
   *         <code>serverName</code> passed is
   *         <code>1.example.org,60030,12345</code>
   */
  public static String getHLogDirectoryName(final String serverName) {
    StringBuilder dirName = new StringBuilder(HConstants.HREGION_LOGDIR_NAME);
    dirName.append("/");
    dirName.append(serverName);
    return dirName.toString();
  }

  /**
   * @param regiondir
   *          This regions directory in the filesystem.
   * @return The directory that holds recovered edits files for the region
   *         <code>regiondir</code>
   */
  public static Path getRegionDirRecoveredEditsDir(final Path regiondir) {
    return new Path(regiondir, HLog.RECOVERED_EDITS_DIR);
  }

  /**
   * Move aside a bad edits file.
   * 
   * @param fs
   * @param edits
   *          Edits file to move aside.
   * @return The name of the moved aside file.
   * @throws IOException
   */
  public static Path moveAsideBadEditsFile(final FileSystem fs, final Path edits)
      throws IOException {
    Path moveAsideName = new Path(edits.getParent(), edits.getName() + "."
        + System.currentTimeMillis());
    if (!fs.rename(edits, moveAsideName)) {
      LOG.warn("Rename failed from " + edits + " to " + moveAsideName);
    }
    return moveAsideName;
  }

  /**
   * @param path
   *          - the path to analyze. Expected format, if it's in hlog directory:
   *          / [base directory for hbase] / hbase / .logs / ServerName /
   *          logfile
   * @return null if it's not a log file. Returns the ServerName of the region
   *         server that created this log file otherwise.
   */
  public static ServerName getServerNameFromHLogDirectoryName(
      Configuration conf, String path) throws IOException {
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

    final String serverName = serverNameAndFile.substring(0,
        serverNameAndFile.indexOf('/') - 1);

    if (!ServerName.isFullServerName(serverName)) {
      return null;
    }

    return ServerName.parseServerName(serverName);
  }

  /**
   * Return regions (memstores) that have edits that are equal or less than the
   * passed <code>oldestWALseqid</code>.
   * 
   * @param oldestWALseqid
   * @param regionsToSeqids
   *          Encoded region names to sequence ids
   * @return All regions whose seqid is < than <code>oldestWALseqid</code> (Not
   *         necessarily in order). Null if no regions found.
   */
  static byte[][] findMemstoresWithEditsEqualOrOlderThan(
      final long oldestWALseqid, final Map<byte[], Long> regionsToSeqids) {
    // This method is static so it can be unit tested the easier.
    List<byte[]> regions = null;
    for (Map.Entry<byte[], Long> e : regionsToSeqids.entrySet()) {
      if (e.getValue().longValue() <= oldestWALseqid) {
        if (regions == null)
          regions = new ArrayList<byte[]>();
        // Key is encoded region name.
        regions.add(e.getKey());
      }
    }
    return regions == null ? null : regions
        .toArray(new byte[][] { HConstants.EMPTY_BYTE_ARRAY });
  }

  /**
   * Returns sorted set of edit files made by wal-log splitter, excluding files
   * with '.temp' suffix.
   * 
   * @param fs
   * @param regiondir
   * @return Files in passed <code>regiondir</code> as a sorted set.
   * @throws IOException
   */
  public static NavigableSet<Path> getSplitEditFilesSorted(final FileSystem fs,
      final Path regiondir) throws IOException {
    NavigableSet<Path> filesSorted = new TreeSet<Path>();
    Path editsdir = HLogUtil.getRegionDirRecoveredEditsDir(regiondir);
    if (!fs.exists(editsdir))
      return filesSorted;
    FileStatus[] files = FSUtils.listStatus(fs, editsdir, new PathFilter() {
      @Override
      public boolean accept(Path p) {
        boolean result = false;
        try {
          // Return files and only files that match the editfile names pattern.
          // There can be other files in this directory other than edit files.
          // In particular, on error, we'll move aside the bad edit file giving
          // it a timestamp suffix. See moveAsideBadEditsFile.
          Matcher m = HLog.EDITFILES_NAME_PATTERN.matcher(p.getName());
          result = fs.isFile(p) && m.matches();
          // Skip the file whose name ends with RECOVERED_LOG_TMPFILE_SUFFIX,
          // because it means splithlog thread is writting this file.
          if (p.getName().endsWith(HLog.RECOVERED_LOG_TMPFILE_SUFFIX)) {
            result = false;
          }
        } catch (IOException e) {
          LOG.warn("Failed isFile check on " + p);
        }
        return result;
      }
    });
    if (files == null)
      return filesSorted;
    for (FileStatus status : files) {
      filesSorted.add(status.getPath());
    }
    return filesSorted;
  }
}
