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

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.Function;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.io.hfile.HFileWriterImpl;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class CustomTieringMultiFileWriter extends DateTieredMultiFileWriter {

  public static final byte[] CUSTOM_TIERING_TIME_RANGE = Bytes.toBytes("CUSTOM_TIERING_TIME_RANGE");

  private NavigableMap<Long, TimeRangeTracker> lowerBoundary2TimeRanger = new TreeMap<>();

  public CustomTieringMultiFileWriter(List<Long> lowerBoundaries,
    Map<Long, String> lowerBoundariesPolicies, boolean needEmptyFile,
    Function<ExtendedCell, Long> tieringFunction) {
    super(lowerBoundaries, lowerBoundariesPolicies, needEmptyFile, tieringFunction);
    for (Long lowerBoundary : lowerBoundaries) {
      lowerBoundary2TimeRanger.put(lowerBoundary, null);
    }
  }

  @Override
  public void append(ExtendedCell cell) throws IOException {
    super.append(cell);
    long tieringValue = tieringFunction.apply(cell);
    Map.Entry<Long, TimeRangeTracker> entry = lowerBoundary2TimeRanger.floorEntry(tieringValue);
    if (entry.getValue() == null) {
      TimeRangeTracker timeRangeTracker = TimeRangeTracker.create(TimeRangeTracker.Type.NON_SYNC);
      timeRangeTracker.setMin(tieringValue);
      timeRangeTracker.setMax(tieringValue);
      lowerBoundary2TimeRanger.put(entry.getKey(), timeRangeTracker);
      ((HFileWriterImpl) lowerBoundary2Writer.get(entry.getKey()).getLiveFileWriter())
        .setTimeRangeTrackerForTiering(() -> timeRangeTracker);
    } else {
      TimeRangeTracker timeRangeTracker = entry.getValue();
      if (timeRangeTracker.getMin() > tieringValue) {
        timeRangeTracker.setMin(tieringValue);
      }
      if (timeRangeTracker.getMax() < tieringValue) {
        timeRangeTracker.setMax(tieringValue);
      }
    }
  }

  @Override
  public List<Path> commitWriters(long maxSeqId, boolean majorCompaction,
    Collection<HStoreFile> storeFiles) throws IOException {
    for (Map.Entry<Long, StoreFileWriter> entry : this.lowerBoundary2Writer.entrySet()) {
      StoreFileWriter writer = entry.getValue();
      if (writer != null) {
        writer.appendFileInfo(CUSTOM_TIERING_TIME_RANGE,
          TimeRangeTracker.toByteArray(lowerBoundary2TimeRanger.get(entry.getKey())));
      }
    }
    return super.commitWriters(maxSeqId, majorCompaction, storeFiles);
  }

}
