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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.CatalogFamilyFormat;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test MetaTableAccessor but without spinning up a cluster.
 * We mock regionserver back and forth (we do spin up a zk cluster).
 */
@Category({MiscTests.class, MediumTests.class})
public class TestMetaTableAccessorNoCluster {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMetaTableAccessorNoCluster.class);

  private static final  HBaseTestingUtility UTIL = new HBaseTestingUtility();

  @Before
  public void before() throws Exception {
    UTIL.startMiniZKCluster();
  }

  @After
  public void after() throws IOException {
    UTIL.shutdownMiniZKCluster();
  }

  @Test
  public void testGetHRegionInfo() throws IOException {
    assertNull(CatalogFamilyFormat.getRegionInfo(new Result()));

    List<Cell> kvs = new ArrayList<>();
    Result r = Result.create(kvs);
    assertNull(CatalogFamilyFormat.getRegionInfo(r));

    byte[] f = HConstants.CATALOG_FAMILY;
    // Make a key value that doesn't have the expected qualifier.
    kvs.add(new KeyValue(HConstants.EMPTY_BYTE_ARRAY, f, HConstants.SERVER_QUALIFIER, f));
    r = Result.create(kvs);
    assertNull(CatalogFamilyFormat.getRegionInfo(r));
    // Make a key that does not have a regioninfo value.
    kvs.add(new KeyValue(HConstants.EMPTY_BYTE_ARRAY, f, HConstants.REGIONINFO_QUALIFIER, f));
    RegionInfo hri = CatalogFamilyFormat.getRegionInfo(Result.create(kvs));
    assertTrue(hri == null);
    // OK, give it what it expects
    kvs.clear();
    RegionInfo metaRegionInfo = RegionInfoBuilder.newBuilder(TableName.META_TABLE_NAME).build();
    kvs.add(new KeyValue(HConstants.EMPTY_BYTE_ARRAY, f, HConstants.REGIONINFO_QUALIFIER,
      RegionInfo.toByteArray(metaRegionInfo)));
    hri = CatalogFamilyFormat.getRegionInfo(Result.create(kvs));
    assertNotNull(hri);
    assertTrue(RegionInfo.COMPARATOR.compare(hri, metaRegionInfo) == 0);
  }
}
