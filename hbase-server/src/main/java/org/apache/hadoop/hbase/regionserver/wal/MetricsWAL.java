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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.util.StringUtils;


/**
 * Class used to push numbers about the WAL into the metrics subsystem.  This will take a
 * single function call and turn it into multiple manipulations of the hadoop metrics system.
 */
@InterfaceAudience.Private
public class MetricsWAL {
  static final Log LOG = LogFactory.getLog(MetricsWAL.class);

  private final MetricsWALSource source;

  public MetricsWAL() {
    source = CompatibilitySingletonFactory.getInstance(MetricsWALSource.class);
  }

  public void finishSync(long time) {
    source.incrementSyncTime(time);
  }

  public void finishAppend(long time, long size) {

    source.incrementAppendCount();
    source.incrementAppendTime(time);
    source.incrementAppendSize(size);

    if (time > 1000) {
      source.incrementSlowAppendCount();
      LOG.warn(String.format("%s took %d ms appending an edit to hlog; len~=%s",
          Thread.currentThread().getName(),
          time,
          StringUtils.humanReadableInt(size)));
    }
  }
}
