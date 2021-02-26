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
package org.apache.hadoop.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupProtos;
import org.apache.hadoop.hbase.rsgroup.RSGroupInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Read rs group information from  <code>hbase:rsgroup</code>.
 */
@InterfaceAudience.Private
public final class RSGroupTableAccessor {

  //Assigned before user tables
  private static final TableName RSGROUP_TABLE_NAME =
      TableName.valueOf(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR, "rsgroup");
  private static final byte[] META_FAMILY_BYTES = Bytes.toBytes("m");
  private static final byte[] META_QUALIFIER_BYTES = Bytes.toBytes("i");

  private RSGroupTableAccessor() {
  }

  public static boolean isRSGroupsEnabled(Connection connection) throws IOException {
    return connection.getAdmin().tableExists(RSGROUP_TABLE_NAME);
  }

  public static List<RSGroupInfo> getAllRSGroupInfo(Connection connection)
      throws IOException {
    try (Table rsGroupTable = connection.getTable(RSGROUP_TABLE_NAME)) {
      List<RSGroupInfo> rsGroupInfos = new ArrayList<>();
      for (Result result : rsGroupTable.getScanner(new Scan())) {
        RSGroupInfo rsGroupInfo = getRSGroupInfo(result);
        if (rsGroupInfo != null) {
          rsGroupInfos.add(rsGroupInfo);
        }
      }
      return rsGroupInfos;
    }
  }

  private static RSGroupInfo getRSGroupInfo(Result result) throws IOException {
    byte[] rsGroupInfo = result.getValue(META_FAMILY_BYTES, META_QUALIFIER_BYTES);
    if (rsGroupInfo == null) {
      return null;
    }
    RSGroupProtos.RSGroupInfo proto =
        RSGroupProtos.RSGroupInfo.parseFrom(rsGroupInfo);
    return ProtobufUtil.toGroupInfo(proto);
  }

  public static RSGroupInfo getRSGroupInfo(Connection connection, byte[] rsGroupName)
      throws IOException {
    try (Table rsGroupTable = connection.getTable(RSGROUP_TABLE_NAME)){
      Result result = rsGroupTable.get(new Get(rsGroupName));
      return getRSGroupInfo(result);
    }
  }
}
