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

import javax.annotation.Nullable;
import java.io.IOException;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.TableState;
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
   * Don't use, state was moved to meta, use MetaTableAccessor instead
   * @deprecated state was moved to meta
   */
  @Deprecated
  @Nullable
  private TableState.State tableState;

  /**
   * Creates TableDescriptor with all fields.
   * @param hTableDescriptor HTableDescriptor to use
   * @param tableState table state
   * @deprecated state was moved to meta
   */
  @Deprecated
  public TableDescriptor(HTableDescriptor hTableDescriptor,
      @Nullable TableState.State tableState) {
    this.hTableDescriptor = hTableDescriptor;
    this.tableState = tableState;
  }

  /**
   * Creates TableDescriptor with Enabled table.
   * @param hTableDescriptor HTableDescriptor to use
   */
  @VisibleForTesting
  public TableDescriptor(HTableDescriptor hTableDescriptor) {
    this(hTableDescriptor, TableState.State.ENABLED);
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
   * @return table state
   * @deprecated state was moved to meta
   */
  @Deprecated
  @Nullable
  public TableState.State getTableState() {
    return tableState;
  }

  /**
   * @param tableState state to set for table
   * @deprecated state was moved to meta
   */
  @Deprecated
  public void setTableState(@Nullable TableState.State tableState) {
    this.tableState = tableState;
  }

  /**
   * Convert to PB.
   */
  @SuppressWarnings("deprecation")
  public HBaseProtos.TableDescriptor convert() {
    HBaseProtos.TableDescriptor.Builder builder = HBaseProtos.TableDescriptor.newBuilder()
        .setSchema(hTableDescriptor.convert());
    if (tableState!= null)
      builder.setState(tableState.convert());
    return builder.build();
  }

  /**
   * Convert from PB
   */
  public static TableDescriptor convert(HBaseProtos.TableDescriptor proto) {
    HTableDescriptor hTableDescriptor = HTableDescriptor.convert(proto.getSchema());
    TableState.State state = proto.hasState()?
        TableState.State.convert(proto.getState())
        :null;
    return new TableDescriptor(hTableDescriptor, state);
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
      ts = builder.mergeFrom(bytes, pblen, bytes.length - pblen).build();
    } catch (InvalidProtocolBufferException e) {
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
    if (tableState != that.tableState) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = hTableDescriptor != null ? hTableDescriptor.hashCode() : 0;
    result = 31 * result + (tableState != null ? tableState.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "TableDescriptor{" +
        "hTableDescriptor=" + hTableDescriptor +
        ", tableState=" + tableState +
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
