package org.apache.hadoop.hbase.consensus.log;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.DaemonThreadFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Log Creator to maintain a pool of logs readily available to use.
 *
 * TODO: change this to be a ConsensusServer level to handle all quorums.
 */
public class TransactionLogCreator {
  private static final Logger LOG = LoggerFactory.getLogger(TransactionLogCreator.class);

  /** We use a single thread by default to create new log files for all quorums */
  private static final ScheduledExecutorService threadPool = Executors.newScheduledThreadPool(1,
      new DaemonThreadFactory("TransactionLogCreator-"));

  /** List of opened random access files to use from. */
  private final LinkedBlockingQueue<RandomAccessLog> futureLogs;
  private int maxEmptyLogFiles = HConstants.RAFT_MAX_NUM_NEW_LOGS;

  /** Log directory where it should create the new files */
  private final String logDirectory;

  /** Stops the creator from issuing/creating new files */
  private volatile boolean isClosed = false;

  /** Tells whether to sync the data always on append or not */
  private final boolean isSync;

  private final Runnable refillTask = new Runnable() {
    @Override
    public void run() {
      while (futureLogs.remainingCapacity() > 0 && !isClosed) {
        try {
          tryGenerateNewLogFile();
        } catch (IOException e) {
          LOG.error("Failed to create log file in " + logDirectory + " . Will retry in " +
              HConstants.RETRY_TRANSACTION_LOG_CREATION_DELAY_IN_SECS + " seconds.", e);
          threadPool.schedule(this, HConstants.RETRY_TRANSACTION_LOG_CREATION_DELAY_IN_SECS, TimeUnit.SECONDS);
          break;
        } catch (Throwable t) {
          LOG.error("Failed to create log file in " + logDirectory +
              " unexpectedly. Aborting!", t);
          break;
        }
      }
    }
  };

  // purely for mocking
  public TransactionLogCreator(final String logDirectory) {
    this(logDirectory, false, null);
  }

  public TransactionLogCreator (String logDirectory, boolean isSync,
                                Configuration conf) {
    if (conf != null) {
      maxEmptyLogFiles = conf.getInt(HConstants.RAFT_MAX_NUM_NEW_LOGS_KEY,
        HConstants.RAFT_MAX_NUM_NEW_LOGS);
    }

    StringBuilder logDirectoryBuilder = new StringBuilder(logDirectory);
    if (!logDirectory.endsWith(HConstants.PATH_SEPARATOR)) {
      logDirectoryBuilder.append(HConstants.PATH_SEPARATOR);
    }
    logDirectoryBuilder.append(HConstants.RAFT_CURRENT_LOG_DIRECTORY_NAME);
    logDirectoryBuilder.append(HConstants.PATH_SEPARATOR);
    this.logDirectory = logDirectoryBuilder.toString();

    this.isSync = isSync;

    futureLogs = new LinkedBlockingQueue<>(maxEmptyLogFiles);

    refillFutureLogs();
  }

  /**
   * Returns the new log from the currently list of open files.
   * @return
   * @throws InterruptedException
   */
  public LogFileInterface getNewLogFile() throws InterruptedException {
    if (isClosed) {
      return null;
    }

    RandomAccessLog file = null;
    while (file == null) {
      file = futureLogs.poll(100, TimeUnit.MILLISECONDS);
    }
    refillFutureLogs();
    return file;
  }

  /**
   * Deletes all the opened files.
   * @throws IOException
   */
  public void close() {
    isClosed = true;
    // Delete all the opened files.
    RandomAccessLog ral;
    while ((ral = futureLogs.poll()) != null) {
      try {
        ral.closeAndDelete();
      } catch (IOException e) {
        LOG.warn("Failed to delete log file in " + logDirectory, e);
      }
    }
  }

  /**
   * Refill future logs queue
   */
  private void refillFutureLogs() {
    threadPool.execute(refillTask);
  }

  private void tryGenerateNewLogFile() throws IOException {
    String fileName = generateNewFileName();
    File newFile = new File(logDirectory + fileName);
    if (!futureLogs.offer(new RandomAccessLog(newFile, isSync))) {
      LOG.debug(logDirectory + " is currently full");
      newFile.delete();
    }
  }

  private String generateNewFileName() {
    return "log_" + System.nanoTime();
  }

  protected String getLogDirectory() {
    return logDirectory;
  }

  public static void setMaxThreadNum(int maxThreadNum) {
    ((ThreadPoolExecutor)threadPool).setMaximumPoolSize(maxThreadNum);
  }
}
