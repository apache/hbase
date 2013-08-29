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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.concurrent.ExecutorService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ClientService.BlockingInterface;
import org.apache.hadoop.hbase.regionserver.RegionServerStoppedException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * Test client behavior w/o setting up a cluster.
 * Mock up cluster emissions.
 */
@Category(SmallTests.class)
public class TestClientNoCluster {
  private static final Log LOG = LogFactory.getLog(TestClientNoCluster.class);
  private Configuration conf;

  @Before
  public void setUp() throws Exception {
    this.conf = HBaseConfiguration.create();
    // Run my HConnection overrides.  Use my little HConnectionImplementation below which
    // allows me insert mocks and also use my Registry below rather than the default zk based
    // one so tests run faster and don't have zk dependency.
    this.conf.set("hbase.client.registry.impl", SimpleRegistry.class.getName());
  }

  /**
   * Simple cluster registry inserted in place of our usual zookeeper based one.
   */
  static class SimpleRegistry implements Registry {
    final ServerName META_HOST = new ServerName("10.10.10.10", 60010, 12345);

    @Override
    public void init(HConnection connection) {
    }

    @Override
    public HRegionLocation getMetaRegionLocation() throws IOException {
      return new HRegionLocation(HRegionInfo.FIRST_META_REGIONINFO, META_HOST);
    }

    @Override
    public String getClusterId() {
      return HConstants.CLUSTER_ID_DEFAULT;
    }

    @Override
    public boolean isTableOnlineState(TableName tableName, boolean enabled)
    throws IOException {
      return enabled;
    }

    @Override
    public int getCurrentNrHRS() throws IOException {
      return 1;
    }
  }

  /**
   * Remove the @Ignore to try out timeout and retry asettings
   * @throws IOException
   */
  @Ignore 
  @Test
  public void testTimeoutAndRetries() throws IOException {
    Configuration localConfig = HBaseConfiguration.create(this.conf);
    // This override mocks up our exists/get call to throw a RegionServerStoppedException.
    localConfig.set("hbase.client.connection.impl", RpcTimeoutConnection.class.getName());
    HTable table = new HTable(localConfig, TableName.META_TABLE_NAME);
    Throwable t = null;
    LOG.info("Start");
    try {
      // An exists call turns into a get w/ a flag.
      table.exists(new Get(Bytes.toBytes("abc")));
    } catch (SocketTimeoutException e) {
      // I expect this exception.
      LOG.info("Got expected exception", e);
      t = e;
    } catch (RetriesExhaustedException e) {
      // This is the old, unwanted behavior.  If we get here FAIL!!!
      fail();
    } finally {
      table.close();
    }
    LOG.info("Stop");
    assertTrue(t != null);
  }

  /**
   * Test that operation timeout prevails over rpc default timeout and retries, etc.
   * @throws IOException
   */
  @Test
  public void testRocTimeout() throws IOException {
    Configuration localConfig = HBaseConfiguration.create(this.conf);
    // This override mocks up our exists/get call to throw a RegionServerStoppedException.
    localConfig.set("hbase.client.connection.impl", RpcTimeoutConnection.class.getName());
    int pause = 10;
    localConfig.setInt("hbase.client.pause", pause);
    localConfig.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 10);
    // Set the operation timeout to be < the pause.  Expectation is that after first pause, we will
    // fail out of the rpc because the rpc timeout will have been set to the operation tiemout
    // and it has expired.  Otherwise, if this functionality is broke, all retries will be run --
    // all ten of them -- and we'll get the RetriesExhaustedException exception.
    localConfig.setInt(HConstants.HBASE_CLIENT_META_OPERATION_TIMEOUT, pause - 1);
    HTable table = new HTable(localConfig, TableName.META_TABLE_NAME);
    Throwable t = null;
    try {
      // An exists call turns into a get w/ a flag.
      table.exists(new Get(Bytes.toBytes("abc")));
    } catch (SocketTimeoutException e) {
      // I expect this exception.
      LOG.info("Got expected exception", e);
      t = e;
    } catch (RetriesExhaustedException e) {
      // This is the old, unwanted behavior.  If we get here FAIL!!!
      fail();
    } finally {
      table.close();
    }
    assertTrue(t != null);
  }

  @Test
  public void testDoNotRetryMetaScanner() throws IOException {
    this.conf.set("hbase.client.connection.impl",
      RegionServerStoppedOnScannerOpenConnection.class.getName());
    MetaScanner.metaScan(this.conf, null);
  }

  @Test
  public void testDoNotRetryOnScanNext() throws IOException {
    this.conf.set("hbase.client.connection.impl",
      RegionServerStoppedOnScannerOpenConnection.class.getName());
    // Go against meta else we will try to find first region for the table on construction which
    // means we'll have to do a bunch more mocking.  Tests that go against meta only should be
    // good for a bit of testing.
    HTable table = new HTable(this.conf, TableName.META_TABLE_NAME);
    ResultScanner scanner = table.getScanner(HConstants.CATALOG_FAMILY);
    try {
      Result result = null;
      while ((result = scanner.next()) != null) {
        LOG.info(result);
      }
    } finally {
      scanner.close();
      table.close();
    }
  }

  @Test
  public void testRegionServerStoppedOnScannerOpen() throws IOException {
    this.conf.set("hbase.client.connection.impl",
      RegionServerStoppedOnScannerOpenConnection.class.getName());
    // Go against meta else we will try to find first region for the table on construction which
    // means we'll have to do a bunch more mocking.  Tests that go against meta only should be
    // good for a bit of testing.
    HTable table = new HTable(this.conf, TableName.META_TABLE_NAME);
    ResultScanner scanner = table.getScanner(HConstants.CATALOG_FAMILY);
    try {
      Result result = null;
      while ((result = scanner.next()) != null) {
        LOG.info(result);
      }
    } finally {
      scanner.close();
      table.close();
    }
  }

  /**
   * Override to shutdown going to zookeeper for cluster id and meta location.
   */
  static class ScanOpenNextThenExceptionThenRecoverConnection
  extends HConnectionManager.HConnectionImplementation {
    final ClientService.BlockingInterface stub;

    ScanOpenNextThenExceptionThenRecoverConnection(Configuration conf,
        boolean managed, ExecutorService pool) throws IOException {
      super(conf, managed);
      // Mock up my stub so open scanner returns a scanner id and then on next, we throw
      // exceptions for three times and then after that, we return no more to scan.
      this.stub = Mockito.mock(ClientService.BlockingInterface.class);
      long sid = 12345L;
      try {
        Mockito.when(stub.scan((RpcController)Mockito.any(),
            (ClientProtos.ScanRequest)Mockito.any())).
          thenReturn(ClientProtos.ScanResponse.newBuilder().setScannerId(sid).build()).
          thenThrow(new ServiceException(new RegionServerStoppedException("From Mockito"))).
          thenReturn(ClientProtos.ScanResponse.newBuilder().setScannerId(sid).
            setMoreResults(false).build());
      } catch (ServiceException e) {
        throw new IOException(e);
      }
    }

    @Override
    public BlockingInterface getClient(ServerName sn) throws IOException {
      return this.stub;
    }
  }

  /**
   * Override to shutdown going to zookeeper for cluster id and meta location.
   */
  static class RegionServerStoppedOnScannerOpenConnection
  extends HConnectionManager.HConnectionImplementation {
    final ClientService.BlockingInterface stub;

    RegionServerStoppedOnScannerOpenConnection(Configuration conf, boolean managed,
        ExecutorService pool, User user) throws IOException {
      super(conf, managed);
      // Mock up my stub so open scanner returns a scanner id and then on next, we throw
      // exceptions for three times and then after that, we return no more to scan.
      this.stub = Mockito.mock(ClientService.BlockingInterface.class);
      long sid = 12345L;
      try {
        Mockito.when(stub.scan((RpcController)Mockito.any(),
            (ClientProtos.ScanRequest)Mockito.any())).
          thenReturn(ClientProtos.ScanResponse.newBuilder().setScannerId(sid).build()).
          thenThrow(new ServiceException(new RegionServerStoppedException("From Mockito"))).
          thenReturn(ClientProtos.ScanResponse.newBuilder().setScannerId(sid).
            setMoreResults(false).build());
      } catch (ServiceException e) {
        throw new IOException(e);
      }
    }

    @Override
    public BlockingInterface getClient(ServerName sn) throws IOException {
      return this.stub;
    }
  }

  /**
   * Override to check we are setting rpc timeout right.
   */
  static class RpcTimeoutConnection
  extends HConnectionManager.HConnectionImplementation {
    final ClientService.BlockingInterface stub;

    RpcTimeoutConnection(Configuration conf, boolean managed, ExecutorService pool, User user)
    throws IOException {
      super(conf, managed);
      // Mock up my stub so an exists call -- which turns into a get -- throws an exception
      this.stub = Mockito.mock(ClientService.BlockingInterface.class);
      try {
        Mockito.when(stub.get((RpcController)Mockito.any(),
            (ClientProtos.GetRequest)Mockito.any())).
          thenThrow(new ServiceException(new RegionServerStoppedException("From Mockito")));
      } catch (ServiceException e) {
        throw new IOException(e);
      }
    }

    @Override
    public BlockingInterface getClient(ServerName sn) throws IOException {
      return this.stub;
    }
  }
}
