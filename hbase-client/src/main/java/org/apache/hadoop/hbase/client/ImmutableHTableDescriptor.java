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
import org.apache.hadoop.hbase.client.TableDescriptorBuilder.ModifyableTableDescriptor;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Read-only table descriptor.
 */
@Deprecated // deprecated for hbase 2.0, remove for hbase 3.0. see HTableDescriptor.
@InterfaceAudience.Private
public class ImmutableHTableDescriptor extends HTableDescriptor {

  @Override
  protected HColumnDescriptor toHColumnDescriptor(ColumnFamilyDescriptor desc) {
    if (desc == null) {
      return null;
    } else if (desc instanceof HColumnDescriptor) {
      return new ImmutableHColumnDescriptor((HColumnDescriptor) desc);
    } else {
      return new ImmutableHColumnDescriptor(desc);
    }
  }
  /*
   * Create an unmodifyable copy of an HTableDescriptor
   * @param desc
   */
  public ImmutableHTableDescriptor(final HTableDescriptor desc) {
    super(desc, false);
  }

  public ImmutableHTableDescriptor(final TableDescriptor desc) {
    super(desc instanceof ModifyableTableDescriptor ?
      (ModifyableTableDescriptor) desc : new ModifyableTableDescriptor(desc.getTableName(), desc));
  }

  @Override
  protected ModifyableTableDescriptor getDelegateeForModification() {
    throw new UnsupportedOperationException("HTableDescriptor is read-only");
  }
}
