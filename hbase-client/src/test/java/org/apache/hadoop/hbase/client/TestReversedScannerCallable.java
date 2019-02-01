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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
@Category({ ClientTests.class, SmallTests.class })
public class TestReversedScannerCallable {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestReversedScannerCallable.class);

  @Mock
  private ConnectionImplementation connection;
  @Mock
  private Scan scan;
  @Mock
  private RpcControllerFactory rpcFactory;
  @Mock
  private RegionLocations regionLocations;

  private final byte[] ROW = Bytes.toBytes("row1");

  @Before
  public void setUp() throws Exception {
    byte[] ROW_BEFORE = ConnectionUtils.createCloseRowBefore(ROW);

    Configuration conf = Mockito.mock(Configuration.class);
    HRegionLocation regionLocation = Mockito.mock(HRegionLocation.class);
    ServerName serverName = Mockito.mock(ServerName.class);

    Mockito.when(connection.getConfiguration()).thenReturn(conf);
    Mockito.when(regionLocations.size()).thenReturn(1);
    Mockito.when(regionLocations.getRegionLocation(0)).thenReturn(regionLocation);
    Mockito.when(regionLocation.getHostname()).thenReturn("localhost");
    Mockito.when(regionLocation.getServerName()).thenReturn(serverName);
    Mockito.when(scan.includeStartRow()).thenReturn(true);
    Mockito.when(scan.getStartRow()).thenReturn(ROW);
  }

  @Test
  public void testPrepareDoesNotUseCache() throws Exception {
    TableName tableName = TableName.valueOf("MyTable");
    Mockito.when(connection.relocateRegion(tableName, ROW, 0)).thenReturn(regionLocations);

    ReversedScannerCallable callable =
        new ReversedScannerCallable(connection, tableName, scan, null, rpcFactory);
    callable.prepare(true);

    Mockito.verify(connection).relocateRegion(tableName, ROW, 0);
  }

  @Test
  public void testPrepareUsesCache() throws Exception {
    TableName tableName = TableName.valueOf("MyTable");
    Mockito.when(connection.locateRegion(tableName, ROW, true, true, 0))
        .thenReturn(regionLocations);

    ReversedScannerCallable callable =
        new ReversedScannerCallable(connection, tableName, scan, null, rpcFactory);
    callable.prepare(false);

    Mockito.verify(connection).locateRegion(tableName, ROW, true, true, 0);
  }
}
