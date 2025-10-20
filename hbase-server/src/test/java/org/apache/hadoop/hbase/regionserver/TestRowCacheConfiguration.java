/*
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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RegionServerTests.class, SmallTests.class })
public class TestRowCacheConfiguration {
  private static final byte[] CF1 = "cf1".getBytes();
  private static final TableName TABLE_NAME = TableName.valueOf("table");
  private final static HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  @Test
  public void testDetermineRowCacheEnabled() throws IOException {
    Configuration conf = TEST_UTIL.getConfiguration();

    HRegion region;

    // Set global config to false
    conf.setBoolean(HConstants.ROW_CACHE_ENABLED_KEY, false);

    region = createRegion(null);
    assertFalse(region.determineRowCacheEnabled());

    region = createRegion(false);
    assertFalse(region.determineRowCacheEnabled());

    region = createRegion(true);
    assertTrue(region.determineRowCacheEnabled());

    // Set global config to true
    conf.setBoolean(HConstants.ROW_CACHE_ENABLED_KEY, true);

    region = createRegion(null);
    assertTrue(region.determineRowCacheEnabled());

    region = createRegion(false);
    assertFalse(region.determineRowCacheEnabled());

    region = createRegion(true);
    assertTrue(region.determineRowCacheEnabled());
  }

  private HRegion createRegion(Boolean rowCacheEnabled) throws IOException {
    ColumnFamilyDescriptor cfd = ColumnFamilyDescriptorBuilder.newBuilder(CF1).build();
    TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(TABLE_NAME).setColumnFamily(cfd);
    if (rowCacheEnabled != null) {
      tdb.setRowCacheEnabled(rowCacheEnabled);
    }
    return TEST_UTIL.createLocalHRegion(tdb.build(), "".getBytes(), "1".getBytes());
  }
}
