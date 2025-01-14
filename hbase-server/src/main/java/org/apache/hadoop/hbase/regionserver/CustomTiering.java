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

import static org.apache.hadoop.hbase.regionserver.CustomTieringMultiFileWriter.CUSTOM_TIERING_TIME_RANGE;

import java.io.IOException;
import java.util.Date;
import org.apache.hadoop.hbase.io.hfile.HFileInfo;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class CustomTiering implements DataTiering {
  private static final Logger LOG = LoggerFactory.getLogger(CustomTiering.class);

  private long getMaxTSFromTimeRange(byte[] hFileTimeRange, String hFileName) {
    try {
      if (hFileTimeRange == null) {
        LOG.debug("Custom cell-based timestamp information not found for file: {}", hFileName);
        return Long.MAX_VALUE;
      }
      long parsedValue = TimeRangeTracker.parseFrom(hFileTimeRange).getMax();
      LOG.debug("Max TS for file {} is {}", hFileName, new Date(parsedValue));
      return parsedValue;
    } catch (IOException e) {
      LOG.error("Error occurred while reading the Custom cell-based timestamp metadata of file: {}",
        hFileName, e);
      return Long.MAX_VALUE;
    }
  }

  public long getTimestamp(HStoreFile hStoreFile) {
    return getMaxTSFromTimeRange(hStoreFile.getMetadataValue(CUSTOM_TIERING_TIME_RANGE),
      hStoreFile.getPath().getName());
  }

  public long getTimestamp(HFileInfo hFileInfo) {
    return getMaxTSFromTimeRange(hFileInfo.get(CUSTOM_TIERING_TIME_RANGE),
      hFileInfo.getHFileContext().getHFileName());
  }
}
