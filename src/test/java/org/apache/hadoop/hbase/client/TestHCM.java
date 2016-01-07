/*
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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HConnectionManager.HConnectionImplementation;
import org.apache.hadoop.hbase.client.HConnectionManager.HConnectionKey;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * This class is for testing HCM features
 */
@Category(MediumTests.class)
public class TestHCM {
  private static final Log LOG = LogFactory.getLog(TestHCM.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final byte[] TABLE_NAME = Bytes.toBytes("test");
  private static final byte[] TABLE_NAME1 = Bytes.toBytes("test1");
  private static final byte[] TABLE_NAME2 = Bytes.toBytes("test2");
  private static final byte[] FAM_NAM = Bytes.toBytes("f");
  private static final byte[] ROW = Bytes.toBytes("bbb");

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * @throws InterruptedException 
   * @throws IllegalAccessException 
   * @throws NoSuchFieldException 
   * @throws ZooKeeperConnectionException 
   * @throws IllegalArgumentException 
   * @throws SecurityException 
   * @see https://issues.apache.org/jira/browse/HBASE-2925
   */
  // Disabling.  Of course this test will OOME using new Configuration each time
  // St.Ack 20110428
  // @Test
  public void testManyNewConnectionsDoesnotOOME()
  throws SecurityException, IllegalArgumentException,
  ZooKeeperConnectionException, NoSuchFieldException, IllegalAccessException,
  InterruptedException {
    createNewConfigurations();
  }

  private static Random _randy = new Random();

  public static void createNewConfigurations() throws SecurityException,
  IllegalArgumentException, NoSuchFieldException,
  IllegalAccessException, InterruptedException, ZooKeeperConnectionException {
    HConnection last = null;
    for (int i = 0; i <= (HConnectionManager.MAX_CACHED_HBASE_INSTANCES * 2); i++) {
      // set random key to differentiate the connection from previous ones
      Configuration configuration = HBaseConfiguration.create();
      configuration.set("somekey", String.valueOf(_randy.nextInt()));
      System.out.println("Hash Code: " + configuration.hashCode());
      HConnection connection = HConnectionManager.getConnection(configuration);
      if (last != null) {
        if (last == connection) {
          System.out.println("!! Got same connection for once !!");
        }
      }
      // change the configuration once, and the cached connection is lost forever:
      //      the hashtable holding the cache won't be able to find its own keys
      //      to remove them, so the LRU strategy does not work.
      configuration.set("someotherkey", String.valueOf(_randy.nextInt()));
      last = connection;
      LOG.info("Cache Size: " + getHConnectionManagerCacheSize());
      Thread.sleep(100);
    }
    Assert.assertEquals(1,
      getHConnectionManagerCacheSize());
  }

  private static int getHConnectionManagerCacheSize(){
    return HConnectionTestingUtility.getConnectionCount();
  }
  
  @Test
  public void testClusterConnection() throws IOException {
    ThreadPoolExecutor otherPool = new ThreadPoolExecutor(1, 1, 5, TimeUnit.SECONDS,
        new SynchronousQueue<Runnable>(), Threads.newDaemonThreadFactory("test-hcm"));

    HConnection con1 = HConnectionManager.createConnection(TEST_UTIL.getConfiguration());
    HConnection con2 = HConnectionManager.createConnection(TEST_UTIL.getConfiguration(), otherPool);
    // make sure the internally created ExecutorService is the one passed
    assertTrue(otherPool == ((HConnectionImplementation) con2).getCurrentBatchPool());

    String tableName = "testClusterConnection";
    TEST_UTIL.createTable(tableName.getBytes(), FAM_NAM).close();
    HTable t = (HTable) con1.getTable(tableName, otherPool);
    // make sure passing a pool to the getTable does not trigger creation of an
    // internal pool
    assertNull("Internal Thread pool should be null",
        ((HConnectionImplementation) con1).getCurrentBatchPool());
    // table should use the pool passed
    assertTrue(otherPool == t.getPool());
    t.close();

    t = (HTable) con2.getTable(tableName);
    // table should use the connectin's internal pool
    assertTrue(otherPool == t.getPool());
    t.close();

    t = (HTable) con2.getTable(Bytes.toBytes(tableName));
    // try other API too
    assertTrue(otherPool == t.getPool());
    t.close();

    t = (HTable) con1.getTable(tableName);
    ExecutorService pool = ((HConnectionImplementation) con1).getCurrentBatchPool();
    // make sure an internal pool was created
    assertNotNull("An internal Thread pool should have been created", pool);
    // and that the table is using it
    assertTrue(t.getPool() == pool);
    t.close();

    t = (HTable) con1.getTable(tableName);
    // still using the *same* internal pool
    assertTrue(t.getPool() == pool);
    t.close();

    con1.close();
    // if the pool was created on demand it should be closed upon connectin
    // close
    assertTrue(pool.isShutdown());

    con2.close();
    // if the pool is passed, it is not closed
    assertFalse(otherPool.isShutdown());
    otherPool.shutdownNow();
  }
  
  @Test
  public void abortingHConnectionRemovesItselfFromHCM() throws Exception {
    // Save off current HConnections
    Map<HConnectionKey, HConnectionImplementation> oldHBaseInstances = 
        new HashMap<HConnectionKey, HConnectionImplementation>();
    oldHBaseInstances.putAll(HConnectionManager.HBASE_INSTANCES);
    
    HConnectionManager.HBASE_INSTANCES.clear();

    try {
      HConnection connection = HConnectionManager.getConnection(TEST_UTIL.getConfiguration());
      connection.abort("test abortingHConnectionRemovesItselfFromHCM", new Exception(
          "test abortingHConnectionRemovesItselfFromHCM"));
      Assert.assertNotSame(connection,
        HConnectionManager.getConnection(TEST_UTIL.getConfiguration()));
    } finally {
      // Put original HConnections back
      HConnectionManager.HBASE_INSTANCES.clear();
      HConnectionManager.HBASE_INSTANCES.putAll(oldHBaseInstances);
    }
  }

  /**
   * Test that when we delete a location using the first row of a region
   * that we really delete it.
   * @throws Exception
   */
  @Test
  public void testRegionCaching() throws Exception{
    HTable table = TEST_UTIL.createTable(TABLE_NAME, FAM_NAM);
    TEST_UTIL.createMultiRegions(table, FAM_NAM);
    Put put = new Put(ROW);
    put.add(FAM_NAM, ROW, ROW);
    table.put(put);
    HConnectionManager.HConnectionImplementation conn =
        (HConnectionManager.HConnectionImplementation)table.getConnection();
    assertNotNull(conn.getCachedLocation(TABLE_NAME, ROW));
    conn.deleteCachedLocation(TABLE_NAME, ROW);
    HRegionLocation rl = conn.getCachedLocation(TABLE_NAME, ROW);
    assertNull("What is this location?? " + rl, rl);
    table.close();
  }

  /**
   * Test that Connection or Pool are not closed when managed externally
   * @throws Exception
   */
  @Test
  public void testConnectionManagement() throws Exception{
    TEST_UTIL.createTable(TABLE_NAME1, FAM_NAM);
    HConnection conn = HConnectionManager.createConnection(TEST_UTIL.getConfiguration());

    HTableInterface table = conn.getTable(TABLE_NAME1);
    table.close();
    assertFalse(conn.isClosed());
    assertFalse(((HTable)table).getPool().isShutdown());
    table = conn.getTable(TABLE_NAME1);
    table.close();
    assertFalse(((HTable)table).getPool().isShutdown());
    conn.close();
    assertTrue(((HTable)table).getPool().isShutdown());
  }

  /**
   * Make sure that {@link Configuration} instances that are essentially the
   * same map to the same {@link HConnection} instance.
   */
  @Test
  public void testConnectionSameness() throws Exception {
    HConnection previousConnection = null;
    for (int i = 0; i < 2; i++) {
      // set random key to differentiate the connection from previous ones
      Configuration configuration = TEST_UTIL.getConfiguration();
      configuration.set("some_key", String.valueOf(_randy.nextInt()));
      LOG.info("The hash code of the current configuration is: "
          + configuration.hashCode());
      HConnection currentConnection = HConnectionManager
          .getConnection(configuration);
      if (previousConnection != null) {
        assertTrue(
            "Did not get the same connection even though its key didn't change",
            previousConnection == currentConnection);
      }
      previousConnection = currentConnection;
      // change the configuration, so that it is no longer reachable from the
      // client's perspective. However, since its part of the LRU doubly linked
      // list, it will eventually get thrown out, at which time it should also
      // close the corresponding {@link HConnection}.
      configuration.set("other_key", String.valueOf(_randy.nextInt()));
    }
  }

  /**
   * Makes sure that there is no leaking of
   * {@link HConnectionManager.TableServers} in the {@link HConnectionManager}
   * class.
   */
  @Test
  public void testConnectionUniqueness() throws Exception {
    int zkmaxconnections = TEST_UTIL.getConfiguration().
      getInt(HConstants.ZOOKEEPER_MAX_CLIENT_CNXNS,
        HConstants.DEFAULT_ZOOKEPER_MAX_CLIENT_CNXNS);
    // Test up to a max that is < the maximum number of zk connections.  If we
    // go above zk connections, we just fall into cycle where we are failing
    // to set up a session and test runs for a long time.
    int maxConnections = Math.min(zkmaxconnections - 1, 20);
    List<HConnection> connections = new ArrayList<HConnection>(maxConnections);
    HConnection previousConnection = null;
    try {
      for (int i = 0; i < maxConnections; i++) {
        // set random key to differentiate the connection from previous ones
        Configuration configuration = new Configuration(TEST_UTIL.getConfiguration());
        configuration.set("some_key", String.valueOf(_randy.nextInt()));
        configuration.set(HConstants.HBASE_CLIENT_INSTANCE_ID,
            String.valueOf(_randy.nextInt()));
        LOG.info("The hash code of the current configuration is: "
            + configuration.hashCode());
        HConnection currentConnection =
          HConnectionManager.getConnection(configuration);
        if (previousConnection != null) {
          assertTrue("Got the same connection even though its key changed!",
              previousConnection != currentConnection);
        }
        // change the configuration, so that it is no longer reachable from the
        // client's perspective. However, since its part of the LRU doubly linked
        // list, it will eventually get thrown out, at which time it should also
        // close the corresponding {@link HConnection}.
        configuration.set("other_key", String.valueOf(_randy.nextInt()));

        previousConnection = currentConnection;
        LOG.info("The current HConnectionManager#HBASE_INSTANCES cache size is: "
            + getHConnectionManagerCacheSize());
        Thread.sleep(50);
        connections.add(currentConnection);
      }
    } finally {
      for (HConnection c: connections) {
        // Clean up connections made so we don't interfere w/ subsequent tests.
        HConnectionManager.deleteConnection(c.getConfiguration());
      }
    }
  }

  @Test
  public void testClosing() throws Exception {
    Configuration configuration =
      new Configuration(TEST_UTIL.getConfiguration());
    configuration.set(HConstants.HBASE_CLIENT_INSTANCE_ID,
        String.valueOf(_randy.nextInt()));

    HConnection c1 = HConnectionManager.createConnection(configuration);
    // We create two connections with the same key.
    HConnection c2 = HConnectionManager.createConnection(configuration);

    HConnection c3 = HConnectionManager.getConnection(configuration);
    HConnection c4 = HConnectionManager.getConnection(configuration);
    assertTrue(c3 == c4);

    c1.close();
    assertTrue(c1.isClosed());
    assertFalse(c2.isClosed());
    assertFalse(c3.isClosed());

    c3.close();
    // still a reference left
    assertFalse(c3.isClosed());
    c3.close();
    assertTrue(c3.isClosed());
    // c3 was removed from the cache
    HConnection c5 = HConnectionManager.getConnection(configuration);
    assertTrue(c5 != c3);

    assertFalse(c2.isClosed());
    c2.close();
    assertTrue(c2.isClosed());
    c5.close();
    assertTrue(c5.isClosed());
  }

  /**
   * Trivial test to verify that nobody messes with
   * {@link HConnectionManager#createConnection(Configuration)}
   */
  @Test
  public void testCreateConnection() throws Exception {
    Configuration configuration = TEST_UTIL.getConfiguration();
    HConnection c1 = HConnectionManager.createConnection(configuration);
    HConnection c2 = HConnectionManager.createConnection(configuration);
    // created from the same configuration, yet they are different
    assertTrue(c1 != c2);
    assertTrue(c1.getConfiguration() == c2.getConfiguration());
    // make sure these were not cached
    HConnection c3 = HConnectionManager.getConnection(configuration);
    assertTrue(c1 != c3);
    assertTrue(c2 != c3);
  }

  /**
   * Tests that a destroyed connection does not have a live zookeeper.
   * Below is timing based.  We put up a connection to a table and then close the connection while
   * having a background thread running that is forcing close of the connection to try and
   * provoke a close catastrophe; we are hoping for a car crash so we can see if we are leaking
   * zk connections.
   * @throws Exception
   */
  @Test
  public void testDeleteForZKConnLeak() throws Exception {
    TEST_UTIL.createTable(TABLE_NAME2, FAM_NAM);
    final Configuration config = HBaseConfiguration.create(TEST_UTIL.getConfiguration());
    config.setInt("zookeeper.recovery.retry", 1);
    config.setInt("zookeeper.recovery.retry.intervalmill", 1000);
    config.setInt("hbase.rpc.timeout", 2000);
    config.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);

    ThreadPoolExecutor pool = new ThreadPoolExecutor(1, 10,
      5, TimeUnit.SECONDS,
      new SynchronousQueue<Runnable>(),
      Threads.newDaemonThreadFactory("test-hcm-delete"));

    pool.submit(new Runnable() {
      @Override
      public void run() {
        while (!Thread.interrupted()) {
          try {
            HConnection conn = HConnectionManager.getConnection(config);
            LOG.info("Connection " + conn);
            HConnectionManager.deleteStaleConnection(conn);
            LOG.info("Connection closed " + conn);
            // TODO: This sleep time should be less than the time that it takes to open and close
            // a table.  Ideally we would do a few runs first to measure.  For now this is
            // timing based; hopefully we hit the bad condition.
            Threads.sleep(10);
          } catch (Exception e) {
          }
        }
      }
    });

    // Use connection multiple times.
    for (int i = 0; i < 30; i++) {
      HConnection c1 = null;
      try {
        c1 = HConnectionManager.getConnection(config);
        LOG.info("HTable connection " + i + " " + c1);
        HTable table = new HTable(TABLE_NAME2, c1, pool);
        table.close();
        LOG.info("HTable connection " + i + " closed " + c1);
      } catch (Exception e) {
        LOG.info("We actually want this to happen!!!!  So we can see if we are leaking zk", e);
      } finally {
        if (c1 != null) {
          if (c1.isClosed()) {
            // cannot use getZooKeeper as method instantiates watcher if null
            Field zkwField = c1.getClass().getDeclaredField("zooKeeper");
            zkwField.setAccessible(true);
            Object watcher = zkwField.get(c1);

            if (watcher != null) {
              if (((ZooKeeperWatcher)watcher).getRecoverableZooKeeper().getState().isAlive()) {
                // non-synchronized access to watcher; sleep and check again in case zk connection
                // hasn't been cleaned up yet.
                Thread.sleep(1000);
                if (((ZooKeeperWatcher) watcher).getRecoverableZooKeeper().getState().isAlive()) {
                  pool.shutdownNow();
                  fail("Live zookeeper in closed connection");
                }
              }
            }
          }
          c1.close();
        }
      }
    }
    pool.shutdownNow();
  }

  @org.junit.Rule
  public org.apache.hadoop.hbase.ResourceCheckerJUnitRule cu =
    new org.apache.hadoop.hbase.ResourceCheckerJUnitRule();
}

