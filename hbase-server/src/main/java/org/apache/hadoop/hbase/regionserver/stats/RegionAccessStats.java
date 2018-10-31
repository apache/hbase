/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */

package org.apache.hadoop.hbase.regionserver.stats;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class RegionAccessStats extends AccessStats {
  
  public RegionAccessStats(TableName table, AccessStatsType accessStatsType, byte[] keyRangeStart,
      byte[] keyRangeEnd, long value) {
    super(table, accessStatsType, keyRangeStart, keyRangeEnd, value);
  }

  public RegionAccessStats(TableName table, AccessStatsType accessStatsType, byte[] keyRangeStart,
      byte[] keyRangeEnd, long time, long value) {
    super(table, accessStatsType, keyRangeStart, keyRangeEnd, time, value);
  }

  private String regionName = null;
  
  public void setRegionName(String regionName) {
    this.regionName = regionName;
  }

  public String getRegionName() {
    return regionName;
  }

  @Override
  protected List<KeyPartDescriptor> getKeyPartDescriptors() {
    List<KeyPartDescriptor> keyList = new ArrayList<>();
    keyList.add(new KeyPartDescriptor("Region", 1));
    keyList.add(new KeyPartDescriptor(regionName, 4));

    return keyList;
  }

  @Override
  protected byte[][] convertKeySuffixToByteArrayList(byte[] keyByteArray, int indexStart) {
    byte[][] byteArrayOfArrays = new byte[2][];

    byte[] regionUid = new byte[1];
    regionUid[0] = keyByteArray[indexStart];

    byte[] regionNameUid = new byte[4];
    for (int i = 0; i < 4; i++) {
      regionNameUid[i] = keyByteArray[i + indexStart + 1];
    }

    byteArrayOfArrays[0] = regionUid;
    byteArrayOfArrays[1] = regionNameUid;

    return byteArrayOfArrays;
  }

  @Override
  protected void setFieldsUsingKeyParts(List<String> strings) {
    regionName = strings.get(1); //second value should be region name
  }

}