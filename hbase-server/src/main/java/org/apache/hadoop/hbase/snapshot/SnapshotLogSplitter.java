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

package org.apache.hadoop.hbase.snapshot;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.HLogLink;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogFactory;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.HLogUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;

/**
 * If the snapshot has references to one or more log files,
 * those must be split (each log contains multiple tables and regions)
 * and must be placed in the region/recovered.edits folder.
 * (recovered.edits files will be played on region startup)
 *
 * In case of Restore: the log can just be split in the recovered.edits folder.
 * In case of Clone: each entry in the log must be modified to use the new region name.
 * (region names are encoded with: tableName, startKey, regionIdTimeStamp)
 *
 * We can't use the normal split code, because the HLogKey contains the
 * table name and the region name, and in case of "clone from snapshot"
 * region name and table name will be different and must be replaced in
 * the recovered.edits.
 */
@InterfaceAudience.Private
class SnapshotLogSplitter implements Closeable {
  static final Log LOG = LogFactory.getLog(SnapshotLogSplitter.class);

  private final class LogWriter implements Closeable {
    private HLog.Writer writer;
    private Path logFile;
    private long seqId;

    public LogWriter(final Configuration conf, final FileSystem fs,
        final Path logDir, long seqId) throws IOException {
      logFile = new Path(logDir, logFileName(seqId, true));
      this.writer = HLogFactory.createWriter(fs, logFile, conf);
      this.seqId = seqId;
    }

    public void close() throws IOException {
      writer.close();

      Path finalFile = new Path(logFile.getParent(), logFileName(seqId, false));
      LOG.debug("LogWriter tmpLogFile=" + logFile + " -> logFile=" + finalFile);
      fs.rename(logFile, finalFile);
    }

    public void append(final HLog.Entry entry) throws IOException {
      writer.append(entry);
      if (seqId < entry.getKey().getLogSeqNum()) {
        seqId = entry.getKey().getLogSeqNum();
      }
    }

    private String logFileName(long seqId, boolean temp) {
      String fileName = String.format("%019d", seqId);
      if (temp) fileName += HLog.RECOVERED_LOG_TMPFILE_SUFFIX;
      return fileName;
    }
  }

  private final Map<byte[], LogWriter> regionLogWriters =
      new TreeMap<byte[], LogWriter>(Bytes.BYTES_COMPARATOR);

  private final Map<byte[], byte[]> regionsMap;
  private final Configuration conf;
  private final TableName snapshotTableName;
  private final TableName tableName;
  private final Path tableDir;
  private final FileSystem fs;

  /**
   * @params tableName snapshot table name
   * @params regionsMap maps original region names to the new ones.
   */
  public SnapshotLogSplitter(final Configuration conf, final FileSystem fs,
      final Path tableDir, final TableName snapshotTableName,
      final Map<byte[], byte[]> regionsMap) {
    this.regionsMap = regionsMap;
    this.snapshotTableName = snapshotTableName;
    this.tableName = FSUtils.getTableName(tableDir);
    this.tableDir = tableDir;
    this.conf = conf;
    this.fs = fs;
  }

  public void close() throws IOException {
    for (LogWriter writer: regionLogWriters.values()) {
      writer.close();
    }
  }

  public void splitLog(final String serverName, final String logfile) throws IOException {
    LOG.debug("Restore log=" + logfile + " server=" + serverName +
              " for snapshotTable=" + snapshotTableName +
              " to table=" + tableName);
    splitLog(new HLogLink(conf, serverName, logfile).getAvailablePath(fs));
  }

  public void splitRecoveredEdit(final Path editPath) throws IOException {
    LOG.debug("Restore recover.edits=" + editPath +
              " for snapshotTable=" + snapshotTableName +
              " to table=" + tableName);
    splitLog(editPath);
  }

  /**
   * Split the snapshot HLog reference into regions recovered.edits.
   *
   * The HLogKey contains the table name and the region name,
   * and they must be changed to the restored table names.
   *
   * @param logPath Snapshot HLog reference path
   */
  public void splitLog(final Path logPath) throws IOException {
    HLog.Reader log = HLogFactory.createReader(fs, logPath, conf);
    try {
      HLog.Entry entry;
      LogWriter writer = null;
      byte[] regionName = null;
      byte[] newRegionName = null;
      while ((entry = log.next()) != null) {
        HLogKey key = entry.getKey();

        // We're interested only in the snapshot table that we're restoring
        if (!key.getTablename().equals(snapshotTableName)) continue;

        // Writer for region.
        if (!Bytes.equals(regionName, key.getEncodedRegionName())) {
          regionName = key.getEncodedRegionName().clone();

          // Get the new region name in case of clone, or use the original one
          newRegionName = regionsMap.get(regionName);
          if (newRegionName == null) newRegionName = regionName;

          writer = getOrCreateWriter(newRegionName, key.getLogSeqNum());
          LOG.debug("+ regionName=" + Bytes.toString(regionName));
        }

        // Append Entry
        key = new HLogKey(newRegionName, tableName,
                          key.getLogSeqNum(), key.getWriteTime(), key.getClusterIds());
        writer.append(new HLog.Entry(key, entry.getEdit()));
      }
    } catch (IOException e) {
      LOG.warn("Something wrong during the log split", e);
    } finally {
      log.close();
    }
  }

  /**
   * Create a LogWriter for specified region if not already created.
   */
  private LogWriter getOrCreateWriter(final byte[] regionName, long seqId) throws IOException {
    LogWriter writer = regionLogWriters.get(regionName);
    if (writer == null) {
      Path regionDir = HRegion.getRegionDir(tableDir, Bytes.toString(regionName));
      Path dir = HLogUtil.getRegionDirRecoveredEditsDir(regionDir);
      fs.mkdirs(dir);

      writer = new LogWriter(conf, fs, dir, seqId);
      regionLogWriters.put(regionName, writer);
    }
    return(writer);
  }
}
