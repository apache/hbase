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
package org.apache.hadoop.hbase.regionserver.throttle;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Helper methods for throttling
 */
@InterfaceAudience.Private
public final class ThroughputControlUtil {
  private ThroughputControlUtil() {
  }

  private static final AtomicInteger NAME_COUNTER = new AtomicInteger(0);
  private static final String NAME_DELIMITER = "#";

  /**
   * Generate a name for throttling, to prevent name conflict when multiple IO operation running
   * parallel on the same store.
   * @param store the Store instance on which IO operation is happening
   * @param opName Name of the IO operation, e.g. "flush", "compaction", etc.
   * @return The name for throttling
   */
  public static String getNameForThrottling(HStore store, String opName) {
    int counter;
    for (;;) {
      counter = NAME_COUNTER.get();
      int next = counter == Integer.MAX_VALUE ? 0 : counter + 1;
      if (NAME_COUNTER.compareAndSet(counter, next)) {
        break;
      }
    }
    return store.getRegionInfo().getEncodedName() + NAME_DELIMITER +
        store.getColumnFamilyDescriptor().getNameAsString() + NAME_DELIMITER + opName +
        NAME_DELIMITER + counter;
  }
}
