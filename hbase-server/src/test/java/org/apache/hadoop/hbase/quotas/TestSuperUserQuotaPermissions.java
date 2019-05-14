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
package org.apache.hadoop.hbase.quotas;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.Waiter.Predicate;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.security.access.AccessControlClient;
import org.apache.hadoop.hbase.security.access.AccessController;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test class to verify that the HBase superuser can override quotas.
 */
@Category(MediumTests.class)
public class TestSuperUserQuotaPermissions {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSuperUserQuotaPermissions.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestSuperUserQuotaPermissions.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  // Default to the user running the tests
  private static final String SUPERUSER_NAME = System.getProperty("user.name");
  private static final UserGroupInformation SUPERUSER_UGI =
      UserGroupInformation.createUserForTesting(SUPERUSER_NAME, new String[0]);
  private static final String REGULARUSER_NAME = "quota_regularuser";
  private static final UserGroupInformation REGULARUSER_UGI =
      UserGroupInformation.createUserForTesting(REGULARUSER_NAME, new String[0]);
  private static final AtomicLong COUNTER = new AtomicLong(0);

  @Rule
  public TestName testName = new TestName();
  private SpaceQuotaHelperForTests helper;

  @BeforeClass
  public static void setupMiniCluster() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    // Increase the frequency of some of the chores for responsiveness of the test
    SpaceQuotaHelperForTests.updateConfigForQuotas(conf);

    conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, AccessController.class.getName());
    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, AccessController.class.getName());
    conf.set(CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY, AccessController.class.getName());
    conf.setBoolean("hbase.security.exec.permission.checks", true);
    conf.setBoolean("hbase.security.authorization", true);
    conf.set("hbase.superuser", SUPERUSER_NAME);

    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void removeAllQuotas() throws Exception {
    final Connection conn = TEST_UTIL.getConnection();
    if (helper == null) {
      helper = new SpaceQuotaHelperForTests(TEST_UTIL, testName, COUNTER);
    }
    // Wait for the quota table to be created
    if (!conn.getAdmin().tableExists(QuotaUtil.QUOTA_TABLE_NAME)) {
      helper.waitForQuotaTable(conn);
    } else {
      // Or, clean up any quotas from previous test runs.
      helper.removeAllQuotas(conn);
      assertEquals(0, helper.listNumDefinedQuotas(conn));
    }
  }

  @Test
  public void testSuperUserCanStillCompact() throws Exception {
    // Create a table and write enough data to push it into quota violation
    final TableName tn = doAsSuperUser(new Callable<TableName>() {
      @Override
      public TableName call() throws Exception {
        try (Connection conn = getConnection()) {
          Admin admin = conn.getAdmin();
          final TableName tn = helper.createTableWithRegions(admin, 5);
          // Grant the normal user permissions
          try {
            AccessControlClient.grant(
                conn, tn, REGULARUSER_NAME, null, null, Action.READ, Action.WRITE);
          } catch (Throwable t) {
            if (t instanceof Exception) {
              throw (Exception) t;
            }
            throw new Exception(t);
          }
          return tn;
        }
      }
    });

    // Write a bunch of data as our end-user
    doAsRegularUser(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        try (Connection conn = getConnection()) {
          // Write way more data so that we have HFiles > numRegions after flushes
          // helper.writeData flushes itself, so no need to flush explicitly
          helper.writeData(tn, 2L * SpaceQuotaHelperForTests.ONE_MEGABYTE);
          helper.writeData(tn, 2L * SpaceQuotaHelperForTests.ONE_MEGABYTE);
          return null;
        }
      }
    });

    final long sizeLimit = 2L * SpaceQuotaHelperForTests.ONE_MEGABYTE;
    QuotaSettings settings = QuotaSettingsFactory.limitTableSpace(
        tn, sizeLimit, SpaceViolationPolicy.NO_WRITES_COMPACTIONS);

    try (Connection conn = getConnection()) {
      conn.getAdmin().setQuota(settings);
    }

    waitForTableToEnterQuotaViolation(tn);

    // Should throw an exception, unprivileged users cannot compact due to the quota
    try {
      doAsRegularUser(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          try (Connection conn = getConnection()) {
            conn.getAdmin().majorCompact(tn);
            return null;
          }
        }
      });
      fail("Expected an exception trying to compact a table with a quota violation");
    } catch (DoNotRetryIOException e) {
      // Expected Exception.
      LOG.debug("message", e);
    }

    try{
      // Should not throw an exception (superuser can do anything)
      doAsSuperUser(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          try (Connection conn = getConnection()) {
            conn.getAdmin().majorCompact(tn);
            return null;
          }
        }
      });
    } catch (Exception e) {
      // Unexpected Exception.
      LOG.debug("message", e);
      fail("Did not expect an exception to be thrown while a super user tries "
          + "to compact a table with a quota violation");
    }
    int numberOfRegions = TEST_UTIL.getAdmin().getRegions(tn).size();
    waitForHFilesCountLessorEqual(tn, Bytes.toBytes("f1"), numberOfRegions);
  }

  @Test
  public void testSuperuserCanRemoveQuota() throws Exception {
    // Create a table and write enough data to push it into quota violation
    final TableName tn = doAsSuperUser(new Callable<TableName>() {
      @Override
      public TableName call() throws Exception {
        try (Connection conn = getConnection()) {
          final Admin admin = conn.getAdmin();
          final TableName tn = helper.createTableWithRegions(admin, 5);
          final long sizeLimit = 2L * SpaceQuotaHelperForTests.ONE_MEGABYTE;
          QuotaSettings settings = QuotaSettingsFactory.limitTableSpace(
              tn, sizeLimit, SpaceViolationPolicy.NO_WRITES_COMPACTIONS);
          admin.setQuota(settings);
          // Grant the normal user permission to create a table and set a quota
          try {
            AccessControlClient.grant(
                conn, tn, REGULARUSER_NAME, null, null, Action.READ, Action.WRITE);
          } catch (Throwable t) {
            if (t instanceof Exception) {
              throw (Exception) t;
            }
            throw new Exception(t);
          }
          return tn;
        }
      }
    });

    // Write a bunch of data as our end-user
    doAsRegularUser(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        try (Connection conn = getConnection()) {
          helper.writeData(tn, 3L * SpaceQuotaHelperForTests.ONE_MEGABYTE);
          return null;
        }
      }
    });

    // Wait for the table to hit quota violation
    waitForTableToEnterQuotaViolation(tn);

    // Try to be "bad" and remove the quota as the end user (we want to write more data!)
    doAsRegularUser(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        try (Connection conn = getConnection()) {
          final Admin admin = conn.getAdmin();
          QuotaSettings qs = QuotaSettingsFactory.removeTableSpaceLimit(tn);
          try {
            admin.setQuota(qs);
            fail("Expected that an unprivileged user should not be allowed to remove a quota");
          } catch (Exception e) {
            // pass
          }
          return null;
        }
      }
    });

    // Verify that the superuser can remove the quota
    doAsSuperUser(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        try (Connection conn = getConnection()) {
          final Admin admin = conn.getAdmin();
          QuotaSettings qs = QuotaSettingsFactory.removeTableSpaceLimit(tn);
          admin.setQuota(qs);
          assertNull(helper.getTableSpaceQuota(conn, tn));
          return null;
        }
      }
    });
  }

  private Connection getConnection() throws IOException {
    return ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
  }

  private <T> T doAsSuperUser(Callable<T> task) throws Exception {
    return doAsUser(SUPERUSER_UGI, task);
  }

  private <T> T doAsRegularUser(Callable<T> task) throws Exception {
    return doAsUser(REGULARUSER_UGI, task);
  }

  private <T> T doAsUser(UserGroupInformation ugi, Callable<T> task) throws Exception {
    return ugi.doAs(new PrivilegedExceptionAction<T>() {
      @Override
      public T run() throws Exception {
        return task.call();
      }
    });
  }

  private void waitForTableToEnterQuotaViolation(TableName tn) throws Exception {
    // Verify that the RegionServer has the quota in violation
    final HRegionServer rs = TEST_UTIL.getHBaseCluster().getRegionServer(0);
    Waiter.waitFor(TEST_UTIL.getConfiguration(), 30 * 1000, 1000, new Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        Map<TableName,SpaceQuotaSnapshot> snapshots =
            rs.getRegionServerSpaceQuotaManager().copyQuotaSnapshots();
        SpaceQuotaSnapshot snapshot = snapshots.get(tn);
        if (snapshot == null) {
          LOG.info("Found no snapshot for " + tn);
          return false;
        }
        LOG.info("Found snapshot " + snapshot);
        return snapshot.getQuotaStatus().isInViolation();
      }
    });
  }

  private void waitForHFilesCountLessorEqual(TableName tn, byte[] cf, int count) throws Exception {
    Waiter.waitFor(TEST_UTIL.getConfiguration(), 30 * 1000, 1000, new Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return TEST_UTIL.getNumHFiles(tn, cf) <= count;
      }
    });
  }
}
