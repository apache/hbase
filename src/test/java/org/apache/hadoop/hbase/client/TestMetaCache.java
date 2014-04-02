/**
 * Copyright 2014 The Apache Software Foundation
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

import junit.framework.Assert;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.StringBytes;
import org.junit.Test;

/**
 * Testcase for MetaCache class.
 */
public class TestMetaCache {

  @Test
  public void testBasic() {
    final StringBytes TABLE = new StringBytes(this.getClass().getSimpleName());
    final StringBytes TABLE2 =
        new StringBytes(this.getClass().getSimpleName() + "2");
    final byte[] START_KEY = Bytes.toBytes("aaa");
    final byte[] END_KEY = Bytes.toBytes("ggg");
    final byte[] SMALL_ROW = Bytes.toBytes("a");
    final byte[] ROW = Bytes.toBytes("ddd");

    final HRegionLocation LOCATION = new HRegionLocation(new HRegionInfo(
        new HTableDescriptor(TABLE.getBytes()), START_KEY, END_KEY),
        new HServerAddress("10.0.0.11:1234"));

    MetaCache metaCache = new MetaCache();

    // Assert initial states
    Assert.assertEquals("getNumber", 0, metaCache.getNumber(TABLE));
    Assert.assertEquals("get(table).size", 0, metaCache.getForTable(TABLE).size());
    Assert.assertEquals("getServers().size()",
        0, metaCache.getServers().size());
    Assert.assertNull("get(table, row)", metaCache.getForRow(TABLE, ROW));

    metaCache.add(TABLE, LOCATION);

    Assert.assertEquals("getNumber", 1, metaCache.getNumber(TABLE));
    Assert.assertEquals("get(table).size", 1, metaCache.getForTable(TABLE).size());
    Assert.assertEquals("getServers().size()",
        1, metaCache.getServers().size());

    Assert.assertEquals("should found", LOCATION, metaCache.getForRow(TABLE, ROW));
    Assert.assertNull("should not found",
        metaCache.getForRow(TABLE, HConstants.EMPTY_BYTE_ARRAY));
    Assert.assertNull("should not found", metaCache.getForRow(TABLE, SMALL_ROW));
    Assert.assertNull("should not found", metaCache.getForRow(TABLE, END_KEY));

    // Add another location of the same table at the same server
    metaCache.add(TABLE, new HRegionLocation(new HRegionInfo(
        new HTableDescriptor(TABLE.getBytes()), HConstants.EMPTY_START_ROW,
        START_KEY), new HServerAddress("10.0.0.11:1234")));

    Assert.assertEquals("should found", LOCATION, metaCache.getForRow(TABLE, ROW));
    Assert.assertNotNull("should found", metaCache.getForRow(TABLE, SMALL_ROW));

    Assert.assertEquals("getNumber", 2, metaCache.getNumber(TABLE));
    Assert.assertEquals("get(table).size", 2, metaCache.getForTable(TABLE).size());
    Assert.assertEquals("getServers().size()",
        1, metaCache.getServers().size());

    // Add another location of the different table at the different server
    metaCache.add(TABLE2, new HRegionLocation(new HRegionInfo(
        new HTableDescriptor(TABLE2.getBytes()), END_KEY,
        HConstants.EMPTY_START_ROW), new HServerAddress("10.0.0.12:1234")));

    Assert.assertEquals("getNumber", 2, metaCache.getNumber(TABLE));
    Assert.assertEquals("get(table).size", 2, metaCache.getForTable(TABLE).size());
    Assert.assertEquals("getServers().size()",
        2, metaCache.getServers().size());

    Assert.assertEquals("getNumber", 1, metaCache.getNumber(TABLE2));
    Assert.assertEquals("get(table).size", 1, metaCache.getForTable(TABLE2).size());

    // Delete the first location with wrong server info
    metaCache.deleteForRow(TABLE, ROW, new HServerAddress("10.0.0.13:1234"));

    Assert.assertEquals("should found", LOCATION, metaCache.getForRow(TABLE, ROW));
    Assert.assertNotNull("should found", metaCache.getForRow(TABLE, SMALL_ROW));

    // Delete the first location with correct server info
    metaCache.deleteForRow(TABLE, ROW, new HServerAddress("10.0.0.11:1234"));

    Assert.assertNull("should not found", metaCache.getForRow(TABLE, ROW));
    Assert.assertNotNull("should found", metaCache.getForRow(TABLE, SMALL_ROW));

    // Delete the second location with null server info
    metaCache.deleteForRow(TABLE, SMALL_ROW, null);

    Assert.assertNull("should not found", metaCache.getForRow(TABLE, ROW));
    Assert.assertNull("should not found", metaCache.getForRow(TABLE, SMALL_ROW));

    Assert.assertEquals("getNumber", 0, metaCache.getNumber(TABLE));
    Assert.assertEquals("get(table).size", 0, metaCache.getForTable(TABLE).size());
    Assert.assertTrue("getServers().size() should <= 2",
        metaCache.getServers().size() <= 2);

    // clear not existing server
    metaCache.clearForServer("10.0.0.13:1234");

    Assert.assertTrue("getServers().size() should <= 2",
        metaCache.getServers().size() <= 2);

    // clear existing server
    metaCache.clearForServer("10.0.0.11:1234");

    Assert.assertTrue("getServers().size() should <= 1",
        metaCache.getServers().size() <= 1);

    // clear all
    metaCache.clear();
    Assert.assertEquals("getServers().size()",
        0, metaCache.getServers().size());

    // Assert not null for get(table) even if not existing
    Assert.assertNotNull("get(table)", metaCache.getForTable(TABLE));
  }

}
