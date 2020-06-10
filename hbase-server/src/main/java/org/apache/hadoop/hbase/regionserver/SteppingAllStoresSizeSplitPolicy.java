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
package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.procedure2.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class SteppingAllStoresSizeSplitPolicy extends SteppingSplitPolicy {
  private static final Logger LOG =
    LoggerFactory.getLogger(SteppingAllStoresSizeSplitPolicy.class);

  /**
   * @return true if sum of store's size exceed the sizeToCheck
   */
  @Override
  protected boolean isExceedSize(int tableRegionsCount, long sizeToCheck) {
    long sumSize = 0;
    for (HStore store : region.getStores()) {
      sumSize += store.getSize();
    }
    if (sumSize > sizeToCheck) {
      LOG.debug("ShouldSplit because region size is big enough " +
        "size={}, sizeToCheck={}, regionsWithCommonTable={}",
        StringUtils.humanSize(sumSize), StringUtils.humanSize(sizeToCheck),
        tableRegionsCount);
      return true;
    }
    return false;
  }

}
