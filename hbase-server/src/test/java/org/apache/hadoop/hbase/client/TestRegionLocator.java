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

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;

@Category({ MediumTests.class, ClientTests.class })
public class TestRegionLocator extends AbstractTestRegionLocator {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionLocator.class);

  @BeforeClass
  public static void setUp() throws Exception {
    startClusterAndCreateTable();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Override
  protected byte[][] getStartKeys(TableName tableName) throws IOException {
    try (RegionLocator locator = UTIL.getConnection().getRegionLocator(tableName)) {
      return locator.getStartKeys();
    }
  }

  @Override
  protected byte[][] getEndKeys(TableName tableName) throws IOException {
    try (RegionLocator locator = UTIL.getConnection().getRegionLocator(tableName)) {
      return locator.getEndKeys();
    }
  }

  @Override
  protected Pair<byte[][], byte[][]> getStartEndKeys(TableName tableName) throws IOException {
    try (RegionLocator locator = UTIL.getConnection().getRegionLocator(tableName)) {
      return locator.getStartEndKeys();
    }
  }

  @Override
  protected HRegionLocation getRegionLocation(TableName tableName, byte[] row, int replicaId)
      throws IOException {
    try (RegionLocator locator = UTIL.getConnection().getRegionLocator(tableName)) {
      return locator.getRegionLocation(row, replicaId);
    }
  }

  @Override
  protected List<HRegionLocation> getRegionLocations(TableName tableName, byte[] row)
      throws IOException {
    try (RegionLocator locator = UTIL.getConnection().getRegionLocator(tableName)) {
      return locator.getRegionLocations(row);
    }
  }

  @Override
  protected List<HRegionLocation> getAllRegionLocations(TableName tableName) throws IOException {
    try (RegionLocator locator = UTIL.getConnection().getRegionLocator(tableName)) {
      return locator.getAllRegionLocations();
    }
  }

  @Override
  protected void clearCache(TableName tableName) throws IOException {
    try (RegionLocator locator = UTIL.getConnection().getRegionLocator(tableName)) {
      locator.clearRegionLocationCache();
    }
  }
}
