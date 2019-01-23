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

import static org.apache.hadoop.hbase.util.FutureUtils.get;

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
public class TestAsyncTableRegionLocator extends AbstractTestRegionLocator {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAsyncTableRegionLocator.class);

  private static AsyncConnection CONN;

  private static AsyncTableRegionLocator LOCATOR;

  @BeforeClass
  public static void setUp() throws Exception {
    startClusterAndCreateTable();
    CONN = ConnectionFactory.createAsyncConnection(UTIL.getConfiguration()).get();
    LOCATOR = CONN.getRegionLocator(TABLE_NAME);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    Closeables.close(CONN, true);
    UTIL.shutdownMiniCluster();
  }

  @Override
  protected byte[][] getStartKeys() throws IOException {
    return get(LOCATOR.getStartKeys()).toArray(new byte[0][]);
  }

  @Override
  protected byte[][] getEndKeys() throws IOException {
    return get(LOCATOR.getEndKeys()).toArray(new byte[0][]);
  }

  @Override
  protected Pair<byte[][], byte[][]> getStartEndKeys() throws IOException {
    List<Pair<byte[], byte[]>> startEndKeys = get(LOCATOR.getStartEndKeys());
    byte[][] startKeys = new byte[startEndKeys.size()][];
    byte[][] endKeys = new byte[startEndKeys.size()][];
    for (int i = 0, n = startEndKeys.size(); i < n; i++) {
      Pair<byte[], byte[]> pair = startEndKeys.get(i);
      startKeys[i] = pair.getFirst();
      endKeys[i] = pair.getSecond();
    }
    return Pair.newPair(startKeys, endKeys);
  }

  @Override
  protected HRegionLocation getRegionLocation(byte[] row, int replicaId) throws IOException {
    return get(LOCATOR.getRegionLocation(row, replicaId));
  }

  @Override
  protected List<HRegionLocation> getRegionLocations(byte[] row) throws IOException {
    return get(LOCATOR.getRegionLocations(row));
  }

  @Override
  protected List<HRegionLocation> getAllRegionLocations() throws IOException {
    return get(LOCATOR.getAllRegionLocations());
  }

  @Override
  protected void clearCache() throws IOException {
    LOCATOR.clearRegionLocationCache();
  }
}
