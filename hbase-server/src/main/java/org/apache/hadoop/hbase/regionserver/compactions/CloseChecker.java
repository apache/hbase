/*
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
package org.apache.hadoop.hbase.regionserver.compactions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Check periodically to see if a system stop is requested
 */
@InterfaceAudience.Private
public class CloseChecker {
  public static final String SIZE_LIMIT_KEY = "hbase.hstore.close.check.interval";
  public static final String TIME_LIMIT_KEY = "hbase.hstore.close.check.time.interval";

  private final int closeCheckSizeLimit;
  private final long closeCheckTimeLimit;

  private long bytesWrittenProgressForCloseCheck;
  private long lastCloseCheckMillis;

  public CloseChecker(Configuration conf, long currentTime) {
    this.closeCheckSizeLimit = conf.getInt(SIZE_LIMIT_KEY, 10 * 1000 * 1000 /* 10 MB */);
    this.closeCheckTimeLimit = conf.getLong(TIME_LIMIT_KEY, 10 * 1000L /* 10 s */);
    this.bytesWrittenProgressForCloseCheck = 0;
    this.lastCloseCheckMillis = currentTime;
  }

  /**
   * Check periodically to see if a system stop is requested every written bytes reach size limit.
   * @return if true, system stop.
   */
  public boolean isSizeLimit(Store store, long bytesWritten) {
    if (closeCheckSizeLimit <= 0) {
      return false;
    }

    bytesWrittenProgressForCloseCheck += bytesWritten;
    if (bytesWrittenProgressForCloseCheck <= closeCheckSizeLimit) {
      return false;
    }

    bytesWrittenProgressForCloseCheck = 0;
    return !store.areWritesEnabled();
  }

  /**
   * Check periodically to see if a system stop is requested every time.
   * @return if true, system stop.
   */
  public boolean isTimeLimit(Store store, long now) {
    if (closeCheckTimeLimit <= 0) {
      return false;
    }

    final long elapsedMillis = now - lastCloseCheckMillis;
    if (elapsedMillis <= closeCheckTimeLimit) {
      return false;
    }

    lastCloseCheckMillis = now;
    return !store.areWritesEnabled();
  }
}
