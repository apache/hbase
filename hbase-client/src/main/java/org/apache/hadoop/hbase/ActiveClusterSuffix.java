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

import java.io.IOException;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ActiveClusterSuffixProtos;

/**
 * The suffix for this cluster. It is serialized to the filesystem and up into zookeeper. This is a
 * container for the id. Also knows how to serialize and deserialize the cluster id.
 */
@InterfaceAudience.Private
public class ActiveClusterSuffix {
  private final String active_cluster_suffix;

  /**
   * New ActiveClusterSuffix.
   */

  public ActiveClusterSuffix(final String cs) {
    this.active_cluster_suffix = cs;
  }

  public String getActiveClusterSuffix() {
    return active_cluster_suffix;
  }

  /** Returns The active cluster suffix serialized using pb w/ pb magic prefix */
  public byte[] toByteArray() {
    return ProtobufUtil.prependPBMagic(convert().toByteArray());
  }

  /**
   * Parse the serialized representation of the {@link ActiveClusterSuffix}
   * @param bytes A pb serialized {@link ActiveClusterSuffix} instance with pb magic prefix
   * @return An instance of {@link ActiveClusterSuffix} made from <code>bytes</code>
   * @see #toByteArray()
   */
  public static ActiveClusterSuffix parseFrom(final byte[] bytes) throws DeserializationException {
    if (ProtobufUtil.isPBMagicPrefix(bytes)) {
      int pblen = ProtobufUtil.lengthOfPBMagic();
      ActiveClusterSuffixProtos.ActiveClusterSuffix.Builder builder =
        ActiveClusterSuffixProtos.ActiveClusterSuffix.newBuilder();
      ActiveClusterSuffixProtos.ActiveClusterSuffix cs = null;
      try {
        ProtobufUtil.mergeFrom(builder, bytes, pblen, bytes.length - pblen);
        cs = builder.build();
      } catch (IOException e) {
        throw new DeserializationException(e);
      }
      return convert(cs);
    } else {
      // Presume it was written out this way, the old way.
      return new ActiveClusterSuffix(Bytes.toString(bytes));
    }
  }

  /** Returns A pb instance to represent this instance. */
  public ActiveClusterSuffixProtos.ActiveClusterSuffix convert() {
    ActiveClusterSuffixProtos.ActiveClusterSuffix.Builder builder =
      ActiveClusterSuffixProtos.ActiveClusterSuffix.newBuilder();
    return builder.setActiveClusterSuffix(this.active_cluster_suffix).build();
  }

  /** Returns A {@link ActiveClusterSuffix} made from the passed in <code>cs</code> */
  public static ActiveClusterSuffix
    convert(final ActiveClusterSuffixProtos.ActiveClusterSuffix cs) {
    return new ActiveClusterSuffix(cs.getActiveClusterSuffix());
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return this.active_cluster_suffix;
  }
}
