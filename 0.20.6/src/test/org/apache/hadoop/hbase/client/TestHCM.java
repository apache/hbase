/*
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
package org.apache.hadoop.hbase.client;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MultiRegionTable;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * This class is for testing HCM features
 */
public class TestHCM extends MultiRegionTable {
  private static final byte[] TABLE_NAME = Bytes.toBytes("test");
  private static final byte[] FAM_NAM = Bytes.toBytes("f");
  private static final byte[] ROW = Bytes.toBytes("bbb");

  public TestHCM() {
    super(Bytes.toString(FAM_NAM));
    desc = new HTableDescriptor(TABLE_NAME);
    desc.addFamily(new HColumnDescriptor(FAM_NAM));
  }

  /**
   * Test that when we delete a location using the first row of a region
   * that we really delete it.
   * @throws Exception
   */
  public void testRegionCaching() throws Exception{
    HTable table = new HTable(TABLE_NAME);
    // We test with the second region because the first has an empty start key
    Put put = new Put(ROW);
    put.add(FAM_NAM, ROW, ROW);
    table.put(put);
    HConnectionManager.TableServers conn =
        (HConnectionManager.TableServers) table.getConnection();
    assertNotNull(conn.getCachedLocation(TABLE_NAME, ROW));
    conn.deleteCachedLocation(TABLE_NAME, ROW);
    HRegionLocation rl = conn.getCachedLocation(TABLE_NAME, ROW);
    assertNull("What is this location?? " + rl, rl);
  }
}
