package org.apache.hadoop.hbase.backup.replication;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.master.cleaner.BaseHFileCleanerDelegate;
import org.apache.hbase.thirdparty.com.google.common.collect.Iterables;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;

@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class ContinuousBackupStagedHFileCleaner extends BaseHFileCleanerDelegate implements Abortable {
  private static final Logger LOG = LoggerFactory.getLogger(ContinuousBackupStagedHFileCleaner.class);

  private boolean stopped = false;
  private boolean aborted;
  private Configuration conf;
  private Connection connection;

  @Override
  public void setConf(Configuration config) {
    this.conf = config;
    this.connection = null;
    try {
      // Establishing HBase connection
      this.connection = ConnectionFactory.createConnection(conf);
      LOG.info("HBase connection established successfully.");
    } catch (IOException ioe) {
      LOG.error("Couldn't establish connection to HBase", ioe);
    }
  }

  @Override
  public Iterable<FileStatus> getDeletableFiles(Iterable<FileStatus> files) {
    if (conf == null) {
      LOG.warn("Configuration is not set. Returning original list of files.");
      return files;
    }

    if (connection == null) {
      try {
        connection = ConnectionFactory.createConnection(conf);
        LOG.info("HBase connection re-established in getDeletableFiles.");
      } catch (IOException e) {
        LOG.error("Failed to re-establish HBase connection. Returning no deletable files.", e);
        return Collections.emptyList();
      }
    }

    try {
      // Fetch staged files from HBase
      Set<String> stagedFiles = StagedBulkloadFileRegistry.listAllBulkloadFiles(connection);
      LOG.debug("Fetched {} staged files from HBase.", stagedFiles.size());
      return Iterables.filter(files, file -> !stagedFiles.contains(file.getPath().toString()));
    } catch (IOException e) {
      LOG.error("Failed to fetch staged bulkload files from HBase. Returning no deletable files.", e);
      return Collections.emptyList();
    }
  }

  @Override
  public boolean isFileDeletable(FileStatus fStat) {
    // The actual deletion decision is made in getDeletableFiles, so returning true
    return true;
  }

  @Override
  public void stop(String why) {
    if (stopped) {
      LOG.debug("Stop method called but the cleaner is already stopped.");
      return;
    }

    if (this.connection != null) {
      try {
        this.connection.close();
        LOG.info("HBase connection closed.");
      } catch (IOException ioe) {
        LOG.debug("Error closing HBase connection", ioe);
      }
    }
    stopped = true;
    LOG.info("ContinuousBackupStagedHFileCleaner stopped. Reason: {}", why);
  }

  @Override
  public boolean isStopped() {
    return stopped;
  }

  @Override
  public void abort(String why, Throwable e) {
    LOG.warn("Aborting ContinuousBackupStagedHFileCleaner because: " + why, e);
    this.aborted = true;
    stop(why);
  }

  @Override
  public boolean isAborted() {
    return this.aborted;
  }
}
