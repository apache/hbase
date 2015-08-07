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
package org.apache.hadoop.hbase;

import java.io.IOException;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.regionserver.BloomType;

/**
 * Class represents table state on HDFS.
 */
@InterfaceAudience.Private
public class TableDescriptor {
  private HTableDescriptor hTableDescriptor;

  /**
   * Creates TableDescriptor with Enabled table.
   * @param hTableDescriptor HTableDescriptor to use
   */
  @VisibleForTesting
  public TableDescriptor(HTableDescriptor hTableDescriptor) {
    this.hTableDescriptor = hTableDescriptor;
  }

  /**
   * Associated HTableDescriptor
   * @return instance of HTableDescriptor
   */
  public HTableDescriptor getHTableDescriptor() {
    return hTableDescriptor;
  }

  public void setHTableDescriptor(HTableDescriptor hTableDescriptor) {
    this.hTableDescriptor = hTableDescriptor;
  }

  /**
   * Convert to PB.
   */
  @SuppressWarnings("deprecation")
  public HBaseProtos.TableDescriptor convert() {
    HBaseProtos.TableDescriptor.Builder builder = HBaseProtos.TableDescriptor.newBuilder()
        .setSchema(hTableDescriptor.convert());
    return builder.build();
  }

  /**
   * Convert from PB
   */
  public static TableDescriptor convert(HBaseProtos.TableDescriptor proto) {
    return new TableDescriptor(HTableDescriptor.convert(proto.getSchema()));
  }

  /**
   * @return This instance serialized with pb with pb magic prefix
   * @see #parseFrom(byte[])
   */
  public byte [] toByteArray() {
    return ProtobufUtil.prependPBMagic(convert().toByteArray());
  }

  /**
   * @param bytes A pb serialized {@link TableDescriptor} instance with pb magic prefix
   * @see #toByteArray()
   */
  public static TableDescriptor parseFrom(final byte [] bytes)
      throws DeserializationException, IOException {
    if (!ProtobufUtil.isPBMagicPrefix(bytes)) {
      throw new DeserializationException("Expected PB encoded TableDescriptor");
    }
    int pblen = ProtobufUtil.lengthOfPBMagic();
    HBaseProtos.TableDescriptor.Builder builder = HBaseProtos.TableDescriptor.newBuilder();
    HBaseProtos.TableDescriptor ts;
    try {
      ProtobufUtil.mergeFrom(builder, bytes, pblen, bytes.length - pblen);
      ts = builder.build();
    } catch (IOException e) {
      throw new DeserializationException(e);
    }
    return convert(ts);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    TableDescriptor that = (TableDescriptor) o;

    if (hTableDescriptor != null ?
        !hTableDescriptor.equals(that.hTableDescriptor) :
        that.hTableDescriptor != null) return false;
    return true;
  }

  @Override
  public int hashCode() {
    return hTableDescriptor != null ? hTableDescriptor.hashCode() : 0;
  }

  @Override
  public String toString() {
    return "TableDescriptor{" +
        "hTableDescriptor=" + hTableDescriptor +
        '}';
  }

  public static HTableDescriptor metaTableDescriptor(final Configuration conf)
      throws IOException {
    HTableDescriptor metaDescriptor = new HTableDescriptor(
        TableName.META_TABLE_NAME,
        new HColumnDescriptor[] {
            new HColumnDescriptor(HConstants.CATALOG_FAMILY)
                .setMaxVersions(conf.getInt(HConstants.HBASE_META_VERSIONS,
                    HConstants.DEFAULT_HBASE_META_VERSIONS))
                .setInMemory(true)
                .setBlocksize(conf.getInt(HConstants.HBASE_META_BLOCK_SIZE,
                    HConstants.DEFAULT_HBASE_META_BLOCK_SIZE))
                .setScope(HConstants.REPLICATION_SCOPE_LOCAL)
                    // Disable blooms for meta.  Needs work.  Seems to mess w/ getClosestOrBefore.
                .setBloomFilterType(BloomType.NONE)
                    // Enable cache of data blocks in L1 if more than one caching tier deployed:
                    // e.g. if using CombinedBlockCache (BucketCache).
                .setCacheDataInL1(true),
            new HColumnDescriptor(HConstants.TABLE_FAMILY)
                // Ten is arbitrary number.  Keep versions to help debugging.
                .setMaxVersions(10)
                .setInMemory(true)
                .setBlocksize(8 * 1024)
                .setScope(HConstants.REPLICATION_SCOPE_LOCAL)
                    // Disable blooms for meta.  Needs work.  Seems to mess w/ getClosestOrBefore.
                .setBloomFilterType(BloomType.NONE)
                    // Enable cache of data blocks in L1 if more than one caching tier deployed:
                    // e.g. if using CombinedBlockCache (BucketCache).
                .setCacheDataInL1(true)
        }) {
    };
    metaDescriptor.addCoprocessor(
        "org.apache.hadoop.hbase.coprocessor.MultiRowMutationEndpoint",
        null, Coprocessor.PRIORITY_SYSTEM, null);
    return metaDescriptor;
  }

}
