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
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

@Category({ MediumTests.class, ClientTests.class })
public class TestRegionLocator extends AbstractTestRegionLocator {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionLocator.class);

  private static RegionLocator LOCATOR;

  @BeforeClass
  public static void setUp() throws Exception {
    startClusterAndCreateTable();
    LOCATOR = UTIL.getConnection().getRegionLocator(TABLE_NAME);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    Closeables.close(LOCATOR, true);
    UTIL.shutdownMiniCluster();
  }

  @Override
  protected byte[][] getStartKeys() throws IOException {
    return LOCATOR.getStartKeys();
  }

  @Override
  protected byte[][] getEndKeys() throws IOException {
    return LOCATOR.getEndKeys();
  }

  @Override
  protected Pair<byte[][], byte[][]> getStartEndKeys() throws IOException {
    return LOCATOR.getStartEndKeys();
  }

  @Override
  protected HRegionLocation getRegionLocation(byte[] row, int replicaId) throws IOException {
    return LOCATOR.getRegionLocation(row, replicaId);
  }

  @Override
  protected List<HRegionLocation> getRegionLocations(byte[] row) throws IOException {
    return LOCATOR.getRegionLocations(row);
  }

  @Override
  protected List<HRegionLocation> getAllRegionLocations() throws IOException {
    return LOCATOR.getAllRegionLocations();
  }

  @Override
  protected void clearCache() throws IOException {
    LOCATOR.clearRegionLocationCache();
  }
}
