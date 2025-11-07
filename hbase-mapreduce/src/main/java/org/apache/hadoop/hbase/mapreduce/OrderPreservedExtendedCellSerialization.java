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
package org.apache.hadoop.hbase.mapreduce;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.OrderPreservedMapReduceExtendedCell;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class OrderPreservedExtendedCellSerialization
  implements Serialization<OrderPreservedMapReduceExtendedCell> {

  @Override
  public boolean accept(Class<?> c) {
    return OrderPreservedMapReduceExtendedCell.class.isAssignableFrom(c);
  }

  @Override
  public Serializer<OrderPreservedMapReduceExtendedCell>
    getSerializer(Class<OrderPreservedMapReduceExtendedCell> c) {
    return new OrderPreservedExtendedCellSerializer();
  }

  @Override
  public Deserializer<OrderPreservedMapReduceExtendedCell>
    getDeserializer(Class<OrderPreservedMapReduceExtendedCell> c) {
    return new OrderPreservedExtendedCellDeserializer();
  }

  public static class OrderPreservedExtendedCellSerializer
    implements Serializer<OrderPreservedMapReduceExtendedCell> {
    private DataOutputStream dos;

    @Override
    public void open(OutputStream os) throws IOException {
      this.dos = new DataOutputStream(os);
    }

    @Override
    public void serialize(OrderPreservedMapReduceExtendedCell kv) throws IOException {
      dos.writeInt(PrivateCellUtil.estimatedSerializedSizeOf(kv) - Bytes.SIZEOF_INT);
      PrivateCellUtil.writeCell(kv, dos, true);
      dos.writeLong(kv.getSequenceId());
      dos.writeInt(kv.getOrder());
    }

    @Override
    public void close() throws IOException {
      dos.close();
    }
  }

  public static class OrderPreservedExtendedCellDeserializer
    implements Deserializer<OrderPreservedMapReduceExtendedCell> {
    private DataInputStream dis;

    @Override
    public void open(InputStream is) throws IOException {
      this.dis = new DataInputStream(is);
    }

    @Override
    public OrderPreservedMapReduceExtendedCell
      deserialize(OrderPreservedMapReduceExtendedCell ignore) throws IOException {
      KeyValue kv = KeyValueUtil.create(this.dis);
      PrivateCellUtil.setSequenceId(kv, this.dis.readLong());
      int order = dis.readInt();
      return new OrderPreservedMapReduceExtendedCell(kv, order);
    }

    @Override
    public void close() throws IOException {
      dis.close();
    }
  }
}
