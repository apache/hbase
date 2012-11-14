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
package org.apache.hadoop.hbase.client;

import org.apache.hadoop.hbase.HConstants;

import java.util.Random;

/**
 * Utility used by client connections such as {@link HConnection} and
 * {@link ServerCallable}
 */
public class ConnectionUtils {

  private static final Random RANDOM = new Random();
  /**
   * Calculate pause time.
   * Built on {@link HConstants#RETRY_BACKOFF}.
   * @param pause
   * @param tries
   * @return How long to wait after <code>tries</code> retries
   */
  public static long getPauseTime(final long pause, final int tries) {
    int ntries = tries;
    if (ntries >= HConstants.RETRY_BACKOFF.length) {
      ntries = HConstants.RETRY_BACKOFF.length - 1;
    }

    long normalPause = pause * HConstants.RETRY_BACKOFF[ntries];
    long jitter =  (long)(normalPause * RANDOM.nextFloat() * 0.01f); // 1% possible jitter
    return normalPause + jitter;
  }
}