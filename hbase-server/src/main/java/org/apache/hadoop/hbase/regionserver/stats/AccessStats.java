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

package org.apache.hadoop.hbase.regionserver.stats;

import java.util.List;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Generic class to hold AccessStats information together, its an abstract class and certain methods
 * needs to be implemented for given granularity of access stats. Currently its implemented only for
 * REGION.
 */
@InterfaceAudience.Private
public abstract class AccessStats {
  protected AccessStatsType accessStatsType;
  TableName table;
  protected long normalizedTimeInEpoch;
  protected long value;
  protected byte[] keyRangeStart;
  protected byte[] keyRangeEnd;

  public AccessStats(TableName table, AccessStatsType accessStatsType, byte[] keyRangeStart,
      byte[] keyRangeEnd, long value) {
    this(table, accessStatsType, keyRangeStart, keyRangeEnd,
        AccessStatsRecorderUtils.getInstance().getNormalizedTimeCurrent(), value);
  }

  public AccessStats(TableName table, AccessStatsType accessStatsType, byte[] keyRangeStart,
      byte[] keyRangeEnd, long time, long value) {
    this.accessStatsType = accessStatsType;
    this.table = table;
    this.normalizedTimeInEpoch = time;
    this.value = value;
    this.keyRangeStart = keyRangeStart;
    this.keyRangeEnd = keyRangeEnd;
  }

  public long getEpochTime() {
    return normalizedTimeInEpoch;
  }

  public byte[] getValueInBytes() {
    return Bytes.toBytes(value);
  }

  public long getValue() {
    return value;
  }

  public byte[] getAccessStatsType() {
    return Bytes.toBytes(accessStatsType.toString());
  }

  public TableName getTable() {
    return table;
  }

  public byte[] getKeyRangeStart() {
    return keyRangeStart;
  }

  public byte[] getKeyRangeEnd() {
    return keyRangeEnd;
  }

  /*
   * Each AccessStats record is uniquely identified by a row key which is a combination of table
   * name and time, followed by fixed set of KeyPartDescriptors; all encoded in fix length binary
   * encoding except time. For a given granularity, this method will return list of
   * KeyPartDescriptor which will be used in generating row key.
   */
  protected abstract List<KeyPartDescriptor> getKeyPartDescriptors();

  /*
   * Since only this class knows how to divide byte encoded row key suffix into different parts,
   * this method is supposed to do that.
   */
  protected abstract byte[][] convertKeySuffixToByteArrayList(byte[] keyByteArray, int indexStart);

  /*
   * This method should set the fields specific to given granularity by using decoded row key parts.
   */
  protected abstract void setFieldsUsingKeyParts(List<String> strings);

  class KeyPartDescriptor {
    private String keyStr;
    private int uidLengthInBytes;

    public KeyPartDescriptor(String keyStr, int uidLengthInBytes) {
      this.keyStr = keyStr;
      this.uidLengthInBytes = uidLengthInBytes;
    }

    public String getKeyStr() {
      return keyStr;
    }

    public int getUidLengthInBytes() {
      return uidLengthInBytes;
    }

  }

  public enum AccessStatsType {
    READCOUNT, WRITECOUNT
  }

  public enum AccessStatsGranularity {
    REGION
  }
}