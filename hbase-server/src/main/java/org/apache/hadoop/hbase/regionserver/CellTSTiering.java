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
package org.apache.hadoop.hbase.regionserver;

import static org.apache.hadoop.hbase.regionserver.HStoreFile.TIMERANGE_KEY;

import java.io.IOException;
import java.util.OptionalLong;
import org.apache.hadoop.hbase.io.hfile.HFileInfo;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class CellTSTiering implements DataTiering {
  private static final Logger LOG = LoggerFactory.getLogger(CellTSTiering.class);

  public long getTimestamp(HStoreFile hStoreFile) {
    OptionalLong maxTimestamp = hStoreFile.getMaximumTimestamp();
    if (!maxTimestamp.isPresent()) {
      LOG.debug("Maximum timestamp not present for {}", hStoreFile.getPath());
      return Long.MAX_VALUE;
    }
    return maxTimestamp.getAsLong();
  }

  public long getTimestamp(HFileInfo hFileInfo) {
    try {
      byte[] hFileTimeRange = hFileInfo.get(TIMERANGE_KEY);
      if (hFileTimeRange == null) {
        LOG.debug("Timestamp information not found for file: {}",
          hFileInfo.getHFileContext().getHFileName());
        return Long.MAX_VALUE;
      }
      return TimeRangeTracker.parseFrom(hFileTimeRange).getMax();
    } catch (IOException e) {
      LOG.error("Error occurred while reading the timestamp metadata of file: {}",
        hFileInfo.getHFileContext().getHFileName(), e);
      return Long.MAX_VALUE;
    }
  }
}
