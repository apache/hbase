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
package org.apache.hadoop.hbase.wal;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.RegionGroupingProvider.RegionGroupingStrategy;

/**
 * A WAL grouping strategy that limits the number of wal groups to
 * "hbase.wal.regiongrouping.numgroups".
 */
@InterfaceAudience.Private
public class BoundedGroupingStrategy implements RegionGroupingStrategy{

  static final String NUM_REGION_GROUPS = "hbase.wal.regiongrouping.numgroups";
  static final int DEFAULT_NUM_REGION_GROUPS = 2;

  private ConcurrentHashMap<String, String> groupNameCache =
      new ConcurrentHashMap<String, String>();
  private AtomicInteger counter = new AtomicInteger(0);
  private String[] groupNames;

  @Override
  public String group(byte[] identifier) {
    String idStr = Bytes.toString(identifier);
    String groupName = groupNameCache.get(idStr);
    if (null == groupName) {
      groupName = groupNames[counter.getAndIncrement() % groupNames.length];
      String extantName = groupNameCache.putIfAbsent(idStr, groupName);
      if (extantName != null) {
        return extantName;
      }
    }
    return groupName;
  }

  @Override
  public void init(Configuration config, String providerId) {
    int regionGroupNumber = config.getInt(NUM_REGION_GROUPS, DEFAULT_NUM_REGION_GROUPS);
    groupNames = new String[regionGroupNumber];
    for (int i = 0; i < regionGroupNumber; i++) {
      groupNames[i] = providerId + GROUP_NAME_DELIMITER + "regiongroup-" + i;
    }
  }

}
