/**
 * Copyright 2010 The Apache Software Foundation
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
package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.idx.IdxColumnDescriptor;
import org.apache.hadoop.hbase.client.idx.IdxIndexDescriptor;
import org.apache.hadoop.hbase.client.idx.IdxQualifierType;

import java.io.IOException;

/**
 * Tests that an IdxRegion is compatible with an HRegion when there are no
 * indexes defined.
 */
public class TestHRegionWithIdxRegion extends TestHRegion {

  /**
   * Override the HRegion inistialization method
   *
   * @param tableName     the table name
   * @param callingMethod the calling method
   * @param conf          the conf (augmented with the IdxRegion as the
   *                      REGION_IMPL
   * @param families      the families
   * @throws IOException exception
   */
  @Override
  protected void initHRegion(byte[] tableName, String callingMethod,
    HBaseConfiguration conf, byte[]... families) throws IOException {
    conf.set(HConstants.REGION_IMPL, IdxRegion.class.getName());
    super.initHRegion(tableName, callingMethod, conf, families);
  }

  @Override
  protected HTableDescriptor constructTableDescriptor(byte[] tableName,
    byte[]... families) {
    HTableDescriptor htd = new HTableDescriptor(tableName);
    for (byte[] family : families) {
      try {
        IdxColumnDescriptor icd = new IdxColumnDescriptor(family);
        icd.addIndexDescriptor(new IdxIndexDescriptor(qual1,
          IdxQualifierType.BYTE_ARRAY));
        icd.addIndexDescriptor(new IdxIndexDescriptor(qual2,
          IdxQualifierType.BYTE_ARRAY));
        icd.addIndexDescriptor(new IdxIndexDescriptor(qual3,
          IdxQualifierType.BYTE_ARRAY));
        htd.addFamily(icd);
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
    }
    return htd;
  }
}