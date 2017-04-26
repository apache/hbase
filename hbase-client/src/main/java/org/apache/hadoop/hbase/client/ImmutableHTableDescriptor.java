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
import org.apache.hadoop.hbase.client.TableDescriptorBuilder.ModifyableTableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Read-only table descriptor.
 */
@Deprecated // deprecated for hbase 2.0, remove for hbase 3.0. see HTableDescriptor.
@InterfaceAudience.Public
public class ImmutableHTableDescriptor extends HTableDescriptor {

  /*
   * Create an unmodifyable copy of an HTableDescriptor
   * @param desc
   */
  public ImmutableHTableDescriptor(final HTableDescriptor desc) {
    super(new UnmodifyableTableDescriptor(desc));
  }

  @Deprecated // deprecated for hbase 2.0, remove for hbase 3.0. see HTableDescriptor.
  private static class UnmodifyableTableDescriptor extends ModifyableTableDescriptor {

    UnmodifyableTableDescriptor(final TableDescriptor desc) {
      super(desc);
    }

    @Override
    protected ModifyableTableDescriptor setFamily(HColumnDescriptor family) {
      throw new UnsupportedOperationException("HTableDescriptor is read-only");
    }

    @Override
    public HColumnDescriptor removeFamily(final byte[] column) {
      throw new UnsupportedOperationException("HTableDescriptor is read-only");
    }

    @Override
    public ModifyableTableDescriptor setValue(final Bytes key, final Bytes value) {
      throw new UnsupportedOperationException("HTableDescriptor is read-only");
    }

    @Override
    public void remove(Bytes key) {
      throw new UnsupportedOperationException("HTableDescriptor is read-only");
    }

    @Override
    public ModifyableTableDescriptor setConfiguration(String key, String value) {
      throw new UnsupportedOperationException("HTableDescriptor is read-only");
    }

    @Override
    public void removeConfiguration(final String key) {
      throw new UnsupportedOperationException("HTableDescriptor is read-only");
    }
  }
}
