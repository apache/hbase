/*
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
package org.apache.hadoop.hbase.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

import java.io.IOException;

/**
 * Utility used composing RegionInfo for 'display'; e.g. on the web UI
 */
@InterfaceAudience.Private
public class RegionInfoDisplay {
  public final static String DISPLAY_KEYS_KEY = "hbase.display.keys";
  public final static byte[] HIDDEN_END_KEY = Bytes.toBytes("hidden-end-key");
  public final static byte[] HIDDEN_START_KEY = Bytes.toBytes("hidden-start-key");

  /**
   * Get the descriptive name as {@link RegionState} does it but with hidden
   * startkey optionally
   * @return descriptive string
   */
  public static String getDescriptiveNameFromRegionStateForDisplay(RegionState state,
                                                                   Configuration conf) {
    if (conf.getBoolean(DISPLAY_KEYS_KEY, true)) return state.toDescriptiveString();
    String descriptiveStringFromState = state.toDescriptiveString();
    int idx = descriptiveStringFromState.lastIndexOf(" state=");
    String regionName = getRegionNameAsStringForDisplay(
    RegionInfoBuilder.newBuilder(state.getRegion()).build(), conf);
    return regionName + descriptiveStringFromState.substring(idx);
  }

  /**
   * Get the end key for display. Optionally hide the real end key.
   * @return the endkey
   */
  public static byte[] getEndKeyForDisplay(RegionInfo ri, Configuration conf) {
    boolean displayKey = conf.getBoolean(DISPLAY_KEYS_KEY, true);
    if (displayKey) return ri.getEndKey();
    return HIDDEN_END_KEY;
  }

  /**
   * Get the start key for display. Optionally hide the real start key.
   * @param ri
   * @param conf
   * @return the startkey
   */
  public static byte[] getStartKeyForDisplay(RegionInfo ri, Configuration conf) {
    boolean displayKey = conf.getBoolean(DISPLAY_KEYS_KEY, true);
    if (displayKey) return ri.getStartKey();
    return HIDDEN_START_KEY;
  }

  /**
   * Get the region name for display. Optionally hide the start key.
   * @param ri
   * @param conf
   * @return region name as String
   */
  public static String getRegionNameAsStringForDisplay(RegionInfo ri, Configuration conf) {
    return Bytes.toStringBinary(getRegionNameForDisplay(ri, conf));
  }

  /**
   * Get the region name for display. Optionally hide the start key.
   * @param ri
   * @param conf
   * @return region name bytes
   */
  public static byte[] getRegionNameForDisplay(RegionInfo ri, Configuration conf) {
    boolean displayKey = conf.getBoolean(DISPLAY_KEYS_KEY, true);
    if (displayKey || ri.getTable().equals(TableName.META_TABLE_NAME)) {
      return ri.getRegionName();
    } else {
      // create a modified regionname with the startkey replaced but preserving
      // the other parts including the encodedname.
      try {
        byte[][]regionNameParts = RegionInfo.parseRegionName(ri.getRegionName());
        regionNameParts[1] = HIDDEN_START_KEY; //replace the real startkey
        int len = 0;
        // get the total length
        for (byte[] b : regionNameParts) {
          len += b.length;
        }
        byte[] encodedRegionName =
        Bytes.toBytes(RegionInfo.encodeRegionName(ri.getRegionName()));
        len += encodedRegionName.length;
        //allocate some extra bytes for the delimiters and the last '.'
        byte[] modifiedName = new byte[len + regionNameParts.length + 1];
        int lengthSoFar = 0;
        int loopCount = 0;
        for (byte[] b : regionNameParts) {
          System.arraycopy(b, 0, modifiedName, lengthSoFar, b.length);
          lengthSoFar += b.length;
          if (loopCount++ == 2) modifiedName[lengthSoFar++] = RegionInfo.REPLICA_ID_DELIMITER;
          else  modifiedName[lengthSoFar++] = HConstants.DELIMITER;
        }
        // replace the last comma with '.'
        modifiedName[lengthSoFar - 1] = RegionInfo.ENC_SEPARATOR;
        System.arraycopy(encodedRegionName, 0, modifiedName, lengthSoFar,
        encodedRegionName.length);
        lengthSoFar += encodedRegionName.length;
        modifiedName[lengthSoFar] = RegionInfo.ENC_SEPARATOR;
        return modifiedName;
      } catch (IOException e) {
        //LOG.warn("Encountered exception " + e);
        throw new RuntimeException(e);
      }
    }
  }
}
