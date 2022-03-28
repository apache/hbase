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
package org.apache.hadoop.hbase.master.cleaner;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for time to live file cleaner.
 */
@InterfaceAudience.Private
public abstract class BaseTimeToLiveFileCleaner extends BaseLogCleanerDelegate {

  private static final Logger LOG =
    LoggerFactory.getLogger(BaseTimeToLiveFileCleaner.class.getName());

  private static final DateTimeFormatter FORMATTER =
    DateTimeFormatter.ISO_DATE_TIME.withZone(ZoneOffset.systemDefault());

  // Configured time a log can be kept after it was closed
  private long ttlMs;

  private volatile boolean stopped = false;

  @Override
  public final void setConf(Configuration conf) {
    super.setConf(conf);
    this.ttlMs = getTtlMs(conf);
  }

  @Override
  public boolean isFileDeletable(FileStatus status) {
    // Files are validated for the second time here,
    // if it causes a bottleneck this logic needs refactored
    if (!valiateFilename(status.getPath())) {
      return true;
    }
    long currentTime = EnvironmentEdgeManager.currentTime();
    long time = status.getModificationTime();
    long life = currentTime - time;

    if (LOG.isTraceEnabled()) {
      LOG.trace("File life:{}ms, ttl:{}ms, current:{}, from{}", life, ttlMs,
        FORMATTER.format(Instant.ofEpochMilli(currentTime)),
        FORMATTER.format(Instant.ofEpochMilli(time)));
    }
    if (life < 0) {
      LOG.warn("Found a file ({}) newer than current time ({} < {}), probably a clock skew",
        status.getPath(), FORMATTER.format(Instant.ofEpochMilli(currentTime)),
        FORMATTER.format(Instant.ofEpochMilli(time)));
      return false;
    }
    return life > ttlMs;
  }

  @Override
  public void stop(String why) {
    this.stopped = true;
  }

  @Override
  public boolean isStopped() {
    return this.stopped;
  }

  protected abstract long getTtlMs(Configuration conf);

  protected abstract boolean valiateFilename(Path file);
}