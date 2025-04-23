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
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Similar to CellSerialization, but includes the sequenceId from an ExtendedCell. This is necessary
 * so that CellSortReducer can sort by sequenceId, if applicable. Note that these two serializations
 * are not compatible -- data serialized by CellSerialization cannot be deserialized with
 * ExtendedCellSerialization and vice versa. This is ok for {@link HFileOutputFormat2} because the
 * serialization is not actually used for the actual written HFiles, just intermediate data (between
 * mapper and reducer of a single job).
 */
@InterfaceAudience.Private
public class ExtendedCellSerialization implements Serialization<ExtendedCell> {
  @Override
  public boolean accept(Class<?> c) {
    return ExtendedCell.class.isAssignableFrom(c);
  }

  @Override
  public ExtendedCellDeserializer getDeserializer(Class<ExtendedCell> t) {
    return new ExtendedCellDeserializer();
  }

  @Override
  public ExtendedCellSerializer getSerializer(Class<ExtendedCell> c) {
    return new ExtendedCellSerializer();
  }

  public static class ExtendedCellDeserializer implements Deserializer<ExtendedCell> {
    private DataInputStream dis;

    @Override
    public void close() throws IOException {
      this.dis.close();
    }

    @Override
    public KeyValue deserialize(ExtendedCell ignore) throws IOException {
      KeyValue kv = KeyValueUtil.create(this.dis);
      PrivateCellUtil.setSequenceId(kv, this.dis.readLong());
      return kv;
    }

    @Override
    public void open(InputStream is) throws IOException {
      this.dis = new DataInputStream(is);
    }
  }

  public static class ExtendedCellSerializer implements Serializer<ExtendedCell> {
    private DataOutputStream dos;

    @Override
    public void close() throws IOException {
      this.dos.close();
    }

    @Override
    public void open(OutputStream os) throws IOException {
      this.dos = new DataOutputStream(os);
    }

    @Override
    public void serialize(ExtendedCell kv) throws IOException {
      dos.writeInt(PrivateCellUtil.estimatedSerializedSizeOf(kv) - Bytes.SIZEOF_INT);
      kv.write(dos, true);
      dos.writeLong(kv.getSequenceId());
    }
  }
}
