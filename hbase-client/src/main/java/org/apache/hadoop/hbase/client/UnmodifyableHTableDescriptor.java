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

package org.apache.hadoop.hbase.client;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * Read-only table descriptor.
 */
@InterfaceAudience.Public
public class UnmodifyableHTableDescriptor extends HTableDescriptor {
  /**
   * Default constructor.
   * @deprecated  As of release 2.0.0. This will be removed in HBase 3.0.0.
   *              Use {@link #UnmodifyableHTableDescriptor(HTableDescriptor)}.
   */
  @Deprecated
  public UnmodifyableHTableDescriptor() {
    super();
  }

  /*
   * Create an unmodifyable copy of an HTableDescriptor
   * @param desc
   */
  UnmodifyableHTableDescriptor(final HTableDescriptor desc) {
    super(desc.getTableName(), getUnmodifyableFamilies(desc), desc.getValues());
  }


  /*
   * @param desc
   * @return Families as unmodifiable array.
   */
  private static HColumnDescriptor[] getUnmodifyableFamilies(
      final HTableDescriptor desc) {
    HColumnDescriptor [] f = new HColumnDescriptor[desc.getFamilies().size()];
    int i = 0;
    for (HColumnDescriptor c: desc.getFamilies()) {
      f[i++] = c;
    }
    return f;
  }

  /**
   * Does NOT add a column family. This object is immutable
   * @param family HColumnDescriptor of familyto add.
   */
  @Override
  public UnmodifyableHTableDescriptor addFamily(final HColumnDescriptor family) {
    throw new UnsupportedOperationException("HTableDescriptor is read-only");
  }

  @Override
  public UnmodifyableHTableDescriptor modifyFamily(HColumnDescriptor family) {
    throw new UnsupportedOperationException("HTableDescriptor is read-only");
  }

  /**
   * @param column
   * @return Column descriptor for the passed family name or the family on
   * passed in column.
   */
  @Override
  public HColumnDescriptor removeFamily(final byte [] column) {
    throw new UnsupportedOperationException("HTableDescriptor is read-only");
  }

  /**
   * @see org.apache.hadoop.hbase.HTableDescriptor#setReadOnly(boolean)
   */
  @Override
  public UnmodifyableHTableDescriptor setReadOnly(boolean readOnly) {
    throw new UnsupportedOperationException("HTableDescriptor is read-only");
  }

  /**
   * @see org.apache.hadoop.hbase.HTableDescriptor#setValue(byte[], byte[])
   */
  @Override
  public UnmodifyableHTableDescriptor setValue(byte[] key, byte[] value) {
    throw new UnsupportedOperationException("HTableDescriptor is read-only");
  }

  /**
   * @see org.apache.hadoop.hbase.HTableDescriptor#setValue(java.lang.String, java.lang.String)
   */
  @Override
  public UnmodifyableHTableDescriptor setValue(String key, String value) {
    throw new UnsupportedOperationException("HTableDescriptor is read-only");
  }

  /**
   * @see org.apache.hadoop.hbase.HTableDescriptor#setMaxFileSize(long)
   */
  @Override
  public UnmodifyableHTableDescriptor setMaxFileSize(long maxFileSize) {
    throw new UnsupportedOperationException("HTableDescriptor is read-only");
  }

  /**
   * @see org.apache.hadoop.hbase.HTableDescriptor#setMemStoreFlushSize(long)
   */
  @Override
  public UnmodifyableHTableDescriptor setMemStoreFlushSize(long memstoreFlushSize) {
    throw new UnsupportedOperationException("HTableDescriptor is read-only");
  }
}
