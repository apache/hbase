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
 * HFile cleaner that uses the timestamp of the hfile to determine if it should be deleted. By
 * default they are allowed to live for {@value #DEFAULT_TTL}
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class TimeToLiveHFileCleaner extends BaseHFileCleanerDelegate {

  private static final Log LOG = LogFactory.getLog(TimeToLiveHFileCleaner.class.getName());
  public static final String TTL_CONF_KEY = "hbase.master.hfilecleaner.ttl";
  // default ttl = 5 minutes
  public static final long DEFAULT_TTL = 60000 * 5;
  // Configured time a hfile can be kept after it was moved to the archive
  private long ttl;

  @Override
  public void setConf(Configuration conf) {
    this.ttl = conf.getLong(TTL_CONF_KEY, DEFAULT_TTL);
    super.setConf(conf);
  }

  @Override
  public boolean isFileDeletable(FileStatus fStat) {
    long currentTime = EnvironmentEdgeManager.currentTime();
    long time = fStat.getModificationTime();
    long life = currentTime - time;
    if (LOG.isTraceEnabled()) {
      LOG.trace("HFile life:" + life + ", ttl:" + ttl + ", current:" + currentTime + ", from: "
          + time);
    }
    if (life < 0) {
      LOG.warn("Found a hfile (" + fStat.getPath() + ") newer than current time (" + currentTime
          + " < " + time + "), probably a clock skew");
      return false;
    }
    return life > ttl;
  }
}
