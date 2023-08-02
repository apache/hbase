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
package org.apache.hadoop.hbase.io.hfile;

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hbase.util.Pair;

import org.apache.hadoop.hbase.shaded.protobuf.generated.PersistentPrefetchProtos;

final class PrefetchProtoUtils {
  private PrefetchProtoUtils() {
  }

  static PersistentPrefetchProtos.PrefetchedHfileName
    toPB(Map<String, Pair<String, Long>> prefetchedHfileNames) {
    Map<String, PersistentPrefetchProtos.RegionFileSizeMap> tmpMap = new HashMap<>();
    prefetchedHfileNames.forEach((hFileName, regionPrefetchMap) -> {
      PersistentPrefetchProtos.RegionFileSizeMap tmpRegionFileSize =
        PersistentPrefetchProtos.RegionFileSizeMap.newBuilder()
          .setRegionName(regionPrefetchMap.getFirst())
          .setRegionPrefetchSize(regionPrefetchMap.getSecond()).build();
      tmpMap.put(hFileName, tmpRegionFileSize);
    });
    return PersistentPrefetchProtos.PrefetchedHfileName.newBuilder().putAllPrefetchedFiles(tmpMap)
      .build();
  }

  static Map<String, Pair<String, Long>>
    fromPB(Map<String, PersistentPrefetchProtos.RegionFileSizeMap> prefetchHFileNames) {
    Map<String, Pair<String, Long>> hFileMap = new HashMap<>();
    prefetchHFileNames.forEach((hFileName, regionPrefetchMap) -> {
      hFileMap.put(hFileName,
        new Pair<>(regionPrefetchMap.getRegionName(), regionPrefetchMap.getRegionPrefetchSize()));
    });
    return hFileMap;
  }
}
