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

package org.apache.hadoop.hbase;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBase170CompatibilityProtos.TableDescriptor;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos.Table;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos.Table.State;

/**
 * Only used for HBase 1.7.0 compatibility sakes.
 */
@InterfaceAudience.Private
public final class DeprecatedTableDescriptor {

  private HTableDescriptor hTableDescriptor;
  private Table tableState;

  private DeprecatedTableDescriptor(HTableDescriptor hTableDescriptor, Table tableState) {
    this.hTableDescriptor = hTableDescriptor;
    this.tableState = tableState;
  }

  public HTableDescriptor getHTableDescriptor() {
    return hTableDescriptor;
  }

  public Table getTableState() {
    return tableState;
  }

  /**
   * Utility method to parse bytes serialized as incompatible TableDescriptors.
   * @param bytes A pb serialized {@link TableDescriptor} instance with pb magic prefix
   */
  public static DeprecatedTableDescriptor parseFrom(final byte [] bytes)
      throws DeserializationException {
    if (!ProtobufUtil.isPBMagicPrefix(bytes)) {
      throw new DeserializationException("Expected PB encoded TableDescriptor");
    }
    int pblen = ProtobufUtil.lengthOfPBMagic();
    TableDescriptor.Builder builder = TableDescriptor.newBuilder();
    TableDescriptor ts;
    try {
      ts = builder.mergeFrom(bytes, pblen, bytes.length - pblen).build();
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }
    return convert(ts);
  }

  private static DeprecatedTableDescriptor convert(TableDescriptor proto) {
    HTableDescriptor hTableDescriptor = HTableDescriptor.convert(proto.getSchema());
    State state = State.valueOf(proto.getState().getNumber());
    Table tableState = Table.newBuilder().setState(state).build();
    return new DeprecatedTableDescriptor(hTableDescriptor, tableState);
  }
}
