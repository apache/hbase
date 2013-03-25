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
package org.apache.hadoop.hbase.snapshot;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Writable version of the SnapshotDescription used by the rpc
 */
public class HSnapshotDescription implements Writable {
  private SnapshotDescription proto;

  public HSnapshotDescription() {
  }

  public HSnapshotDescription(final SnapshotDescription proto) {
    assert proto != null : "proto must be non-null";
    this.proto = proto;
  }

  public String getName() {
    return this.proto.getName();
  }

  public SnapshotDescription getProto() {
    return this.proto;
  }

  public SnapshotDescription.Type getType() {
    return this.proto.getType();
  }

  public String getTable() {
    return this.proto.getTable();
  }

  public boolean hasTable() {
    return this.proto.hasTable();
  }

  public long getCreationTime() {
    return this.proto.getCreationTime();
  }

  public int getVersion() {
    return this.proto.getVersion();
  }

  public String toString() {
    if (this.proto != null) {
      return this.proto.toString();
    }
    return "(no snapshot)";
  }

  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof HSnapshotDescription)) {
      return false;
    }
    SnapshotDescription oproto = ((HSnapshotDescription)obj).getProto();
    if (this.proto == oproto) {
      return true;
    }
    if (this.proto == null && oproto != null) {
      return false;
    }
    return this.proto.equals(oproto);
  }

  // Writable
  /**
   * <em> INTERNAL </em> This method is a part of {@link Writable} interface
   * and is used for de-serialization of the HTableDescriptor over RPC
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    byte[] data = Bytes.readByteArray(in);
    if (data.length > 0) {
      this.proto = SnapshotDescription.parseFrom(data);
    } else {
      this.proto = null;
    }
  }

  /**
   * <em> INTERNAL </em> This method is a part of {@link Writable} interface
   * and is used for serialization of the HTableDescriptor over RPC
   */
  @Override
  public void write(DataOutput out) throws IOException {
    if (this.proto != null) {
      Bytes.writeByteArray(out, this.proto.toByteArray());
    } else {
      Bytes.writeByteArray(out, new byte[0]);
    }
  }
}
