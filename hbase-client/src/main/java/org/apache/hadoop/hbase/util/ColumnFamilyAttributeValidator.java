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
package org.apache.hadoop.hbase.util;

import static org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder.BLOCKSIZE;
import static org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder.BLOOMFILTER;
import static org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder.COMPRESSION;
import static org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder.COMPRESSION_COMPACT;
import static org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder.DATA_BLOCK_ENCODING;
import static org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder.DFS_REPLICATION;
import static org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder.IN_MEMORY_COMPACTION;
import static org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder.KEEP_DELETED_CELLS;
import static org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder.MAX_VERSIONS;
import static org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder.MIN_VERSIONS;
import static org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder.MOB_COMPACT_PARTITION_POLICY;
import static org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder.MOB_THRESHOLD;
import static org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder.REPLICATION_SCOPE;
import static org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder.TTL;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.MemoryCompactionPolicy;
import org.apache.hadoop.hbase.client.MobCompactPartitionPolicy;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class ColumnFamilyAttributeValidator {
  private static final Logger LOG = LoggerFactory.getLogger(ColumnFamilyAttributeValidator.class);
  private final static Map<Bytes, Function<Bytes,Object>> ATTRIBUTE_VALIDATOR = new HashMap<>();

  static {
    ATTRIBUTE_VALIDATOR.put(new Bytes(Bytes.toBytes(DATA_BLOCK_ENCODING)),
      (d) -> DataBlockEncoding.valueOf(d.toString()));
    ATTRIBUTE_VALIDATOR.put(new Bytes(Bytes.toBytes(COMPRESSION)),
      (ca) -> Compression.Algorithm.valueOf(ca.toString()));
    ATTRIBUTE_VALIDATOR.put(new Bytes(Bytes.toBytes(BLOOMFILTER)),
      (bl) -> BloomType.valueOf(bl.toString()));
    ATTRIBUTE_VALIDATOR.put(new Bytes(Bytes.toBytes(REPLICATION_SCOPE)),
      (rs) -> Integer.valueOf(rs.toString()));
    ATTRIBUTE_VALIDATOR.put(new Bytes(Bytes.toBytes(MIN_VERSIONS)),
      (min) -> Integer.valueOf(min.toString()));
    ATTRIBUTE_VALIDATOR.put(new Bytes(Bytes.toBytes(MAX_VERSIONS)),
      (max) -> Integer.valueOf(max.toString()));
    ATTRIBUTE_VALIDATOR.put(new Bytes(Bytes.toBytes(BLOCKSIZE)),
      (bs) -> Integer.valueOf(bs.toString()));
    ATTRIBUTE_VALIDATOR.put(new Bytes(Bytes.toBytes(KEEP_DELETED_CELLS)),
      (k) -> KeepDeletedCells.valueOf(k.toString()));
    ATTRIBUTE_VALIDATOR.put(new Bytes(Bytes.toBytes(TTL)),
      (ttl) -> Integer.valueOf(ttl.toString()));
    ATTRIBUTE_VALIDATOR.put(new Bytes(Bytes.toBytes(DFS_REPLICATION)),
      (dr) -> Integer.valueOf(dr.toString()));
    ATTRIBUTE_VALIDATOR.put(new Bytes(Bytes.toBytes(MOB_THRESHOLD)),
      (mt) -> Long.valueOf(mt.toString()));
    ATTRIBUTE_VALIDATOR.put(new Bytes(Bytes.toBytes(MOB_COMPACT_PARTITION_POLICY)),
      (mc) -> MobCompactPartitionPolicy.valueOf(mc.toString()));
    ATTRIBUTE_VALIDATOR.put(new Bytes(Bytes.toBytes(IN_MEMORY_COMPACTION)),
      (im) -> MemoryCompactionPolicy.valueOf(im.toString()));
    ATTRIBUTE_VALIDATOR.put(new Bytes(Bytes.toBytes(COMPRESSION_COMPACT)),
      (ca) -> Compression.Algorithm.valueOf(ca.toString()));
  }

  /**
   * Validates values of HBase defined column family attributes.
   * Throws {@link IllegalArgumentException} if the value of attribute is not proper format.
   * @param key
   * @param value
   */
  public static void validateAttributeValue(Bytes key, Bytes value) {
    if(ATTRIBUTE_VALIDATOR.containsKey(key)) {
      try {
        ATTRIBUTE_VALIDATOR.get(key).apply(value);
      } catch (Exception e) {
        LOG.error(key.toString() + " attribute value "+ (value != null ? value.toString() : "")
          + " is not valid.", e);
        throw e;
      }
    }
  }
}
