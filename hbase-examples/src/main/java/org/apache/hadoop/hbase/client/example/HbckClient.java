package org.apache.hadoop.hbase.client.example;
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class HbckClient {

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    try(AsyncConnection conn = ConnectionFactory.createAsyncConnection(conf).get()){
      RegionInfo info = new RegionInfo() {
        @Override public String getShortNameToLog() {
          return null;
        }

        @Override public long getRegionId() {
          return 0;
        }

        @Override public byte[] getRegionName() {
          return new byte[0];
        }

        @Override public String getRegionNameAsString() {
          return null;
        }

        @Override public String getEncodedName() {
          return args[0];
        }

        @Override public byte[] getEncodedNameAsBytes() {
          return Bytes.toBytes(args[0]);
        }

        @Override public byte[] getStartKey() {
          return new byte[0];
        }

        @Override public byte[] getEndKey() {
          return new byte[0];
        }

        @Override public TableName getTable() {
          return TableName.valueOf("fake-name");
        }

        @Override public int getReplicaId() {
          return 0;
        }

        @Override public boolean isSplit() {
          return false;
        }

        @Override public boolean isOffline() {
          return false;
        }

        @Override public boolean isSplitParent() {
          return false;
        }

        @Override public boolean isMetaRegion() {
          return false;
        }

        @Override public boolean containsRange(byte[] rangeStartKey, byte[] rangeEndKey) {
          return false;
        }

        @Override public boolean containsRow(byte[] row) {
          return false;
        }
      };
      RegionState state = RegionState.createForTesting(info, RegionState.State.valueOf(args[1]));
      RegionState result = conn.getHbck().get().setRegionStateInMeta(state);
      System.out.println("Successfully changed region from state " + result.getState().name() +
        " to state " + args[1]);
    }

  }
}
