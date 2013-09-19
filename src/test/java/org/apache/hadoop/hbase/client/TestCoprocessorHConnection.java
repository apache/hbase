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

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category(MediumTests.class)
public class TestCoprocessorHConnection {

  private static HBaseTestingUtility UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setupCluster() throws Exception {
    UTIL.startMiniCluster();
  }

  @AfterClass
  public static void shutdownCluster() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  /**
   * Ensure that if the HRegion we are looking up isn't on this server (and not in the cache), that
   * we still correctly look it up.
   * @throws Exception on failure
   */
  @Test
  public void testNonServerLocalLookup() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    // make a fake server that should never be called
    HRegionServer server = Mockito.mock(HRegionServer.class);
    ServerName name = new ServerName("not.a.server.hostname", 12345, -1L);
    Mockito.when(server.getServerName()).thenReturn(name);
    CoprocessorHConnection connection = new CoprocessorHConnection(conf, server);

    // make sure we get the mock server when doing a direct lookup
    assertEquals("Didn't get the mock server from the connection", server,
      connection.getHRegionConnection(name.getHostname(), name.getPort()));

    // create a table that exists
    byte[] tableName = Bytes.toBytes("testNonServerLocalLookup");
    byte[] family = Bytes.toBytes("family");
    UTIL.createTable(tableName, family);

    // if we can write to the table correctly, then our connection is doing the right thing
    HTable table = new HTable(tableName, connection);
    Put p = new Put(Bytes.toBytes("row"));
    p.add(family, null, null);
    table.put(p);
    table.flushCommits();


    // cleanup
    table.close();
    connection.close();
  }

  @Test
  public void testLocalServerLookup() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    // get a real rs
    HRegionServer server =
        UTIL.getMiniHBaseCluster().getLiveRegionServerThreads().get(0).getRegionServer();
    // fake the connection to look like we are living on that server
    CoprocessorHConnection connection = new CoprocessorHConnection(conf, server);

    // create a table that exists
    byte[] tableName = Bytes.toBytes("testLocalServerLookup");
    byte[] family = Bytes.toBytes("family");
    UTIL.createTable(tableName, family);

    // if we can write to the table correctly, then our connection is doing the right thing
    HTable table = new HTable(tableName, connection);
    Put p = new Put(Bytes.toBytes("row"));
    p.add(family, null, null);
    table.put(p);
    table.flushCommits();

    //make sure we get the actual server when doing a direct lookup
    ServerName name = server.getServerName();
    assertEquals("Didn't get the expected server from the connection", server,
      connection.getHRegionConnection(name.getHostname(), name.getPort()));

    // cleanup
    table.close();
    connection.close();
  }
}