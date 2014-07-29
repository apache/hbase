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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * Log cleaner that uses the timestamp of the wal to determine if it should
 * be deleted. By default they are allowed to live for 10 minutes.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class TimeToLiveLogCleaner extends BaseLogCleanerDelegate {
  static final Log LOG = LogFactory.getLog(TimeToLiveLogCleaner.class.getName());
  // Configured time a log can be kept after it was closed
  private long ttl;
  private boolean stopped = false;

  @Override
  public boolean isLogDeletable(FileStatus fStat) {
    long currentTime = EnvironmentEdgeManager.currentTime();
    long time = fStat.getModificationTime();
    long life = currentTime - time;
    
    if (LOG.isTraceEnabled()) {
      LOG.trace("Log life:" + life + ", ttl:" + ttl + ", current:" + currentTime + ", from: "
          + time);
    }
    if (life < 0) {
      LOG.warn("Found a log (" + fStat.getPath() + ") newer than current time (" + currentTime
          + " < " + time + "), probably a clock skew");
      return false;
    }
    return life > ttl;
  }

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    this.ttl = conf.getLong("hbase.master.logcleaner.ttl", 600000);
  }


  @Override
  public void stop(String why) {
    this.stopped = true;
  }

  @Override
  public boolean isStopped() {
    return this.stopped;
  }
}
