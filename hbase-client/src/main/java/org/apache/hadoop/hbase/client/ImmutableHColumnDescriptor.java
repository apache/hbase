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
package org.apache.hadoop.hbase.client;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor;

/**
 * Read-only column descriptor.
 */
@Deprecated // deprecated for hbase 2.0, remove for hbase 3.0. see HColumnDescriptor.
@InterfaceAudience.Private
public class ImmutableHColumnDescriptor extends HColumnDescriptor {
  /*
   * Create an unmodifyable copy of an HColumnDescriptor
   * @param desc
   */
  ImmutableHColumnDescriptor(final HColumnDescriptor desc) {
    super(desc, false);
  }

  public ImmutableHColumnDescriptor(final ColumnFamilyDescriptor desc) {
    super(desc instanceof ModifyableColumnFamilyDescriptor ?
      (ModifyableColumnFamilyDescriptor) desc : new ModifyableColumnFamilyDescriptor(desc));
  }

  @Override
  protected ModifyableColumnFamilyDescriptor getDelegateeForModification() {
    throw new UnsupportedOperationException("HColumnDescriptor is read-only");
  }
}
