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

package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.security.PrivilegedAction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompatibilityFactory;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.test.MetricsAssertHelper;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({RegionServerTests.class, LargeTests.class})
public class TestMetricsUserAggregate {

  @ClassRule public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMetricsUserAggregate.class);

  private static MetricsAssertHelper HELPER =
      CompatibilityFactory.getInstance(MetricsAssertHelper.class);

  private MetricsRegionServerWrapperStub wrapper;
  private MetricsRegionServer rsm;
  private MetricsUserAggregate userAgg;
  private TableName tableName = TableName.valueOf("testUserAggregateMetrics");

  @BeforeClass
  public static void classSetUp() {
    HELPER.init();
  }

  @Before
  public void setUp() {
    wrapper = new MetricsRegionServerWrapperStub();
    Configuration conf = HBaseConfiguration.create();
    rsm = new MetricsRegionServer(wrapper,conf , null);
    userAgg = (MetricsUserAggregate)rsm.getMetricsUserAggregate();
  }

  private void doOperations() {
    for (int i=0; i < 10; i ++) {
      rsm.updateGet(tableName,10);
    }
    for (int i=0; i < 11; i ++) {
      rsm.updateScanTime(tableName,11);
    }
    for (int i=0; i < 12; i ++) {
      rsm.updatePut(tableName,12);
    }
    for (int i=0; i < 13; i ++) {
      rsm.updateDelete(tableName,13);
    }
    for (int i=0; i < 14; i ++) {
      rsm.updateIncrement(tableName,14);
    }
    for (int i=0; i < 15; i ++) {
      rsm.updateAppend(tableName,15);
    }
    for (int i=0; i < 16; i ++) {
      rsm.updateReplay(16);
    }
  }

  @Test
  public void testPerUserOperations() {
    Configuration conf = HBaseConfiguration.create();
    // If metrics for users is not enabled, this test doesn't  make sense.
    if (!conf.getBoolean(MetricsUserAggregateFactory.METRIC_USER_ENABLED_CONF,
      MetricsUserAggregateFactory.DEFAULT_METRIC_USER_ENABLED_CONF)) {
      return;
    }
    User userFoo = User.createUserForTesting(conf, "FOO", new String[0]);
    User userBar = User.createUserForTesting(conf, "BAR", new String[0]);

    userFoo.getUGI().doAs(new PrivilegedAction<Void>() {
      @Override
      public Void run() {
        doOperations();
        return null;
      }
    });

    userBar.getUGI().doAs(new PrivilegedAction<Void>() {
      @Override
      public Void run() {
        doOperations();
        return null;
      }
    });

    HELPER.assertCounter("userfoometricgetnumops", 10, userAgg.getSource());
    HELPER.assertCounter("userfoometricscantimenumops", 11, userAgg.getSource());
    HELPER.assertCounter("userfoometricputnumops", 12, userAgg.getSource());
    HELPER.assertCounter("userfoometricdeletenumops", 13, userAgg.getSource());
    HELPER.assertCounter("userfoometricincrementnumops", 14, userAgg.getSource());
    HELPER.assertCounter("userfoometricappendnumops", 15, userAgg.getSource());
    HELPER.assertCounter("userfoometricreplaynumops", 16, userAgg.getSource());

    HELPER.assertCounter("userbarmetricgetnumops", 10, userAgg.getSource());
    HELPER.assertCounter("userbarmetricscantimenumops", 11, userAgg.getSource());
    HELPER.assertCounter("userbarmetricputnumops", 12, userAgg.getSource());
    HELPER.assertCounter("userbarmetricdeletenumops", 13, userAgg.getSource());
    HELPER.assertCounter("userbarmetricincrementnumops", 14, userAgg.getSource());
    HELPER.assertCounter("userbarmetricappendnumops", 15, userAgg.getSource());
    HELPER.assertCounter("userbarmetricreplaynumops", 16, userAgg.getSource());
  }

  @Test public void testLossyCountingOfUserMetrics() {
    Configuration conf = HBaseConfiguration.create();
    // If metrics for users is not enabled, this test doesn't  make sense.
    if (!conf.getBoolean(MetricsUserAggregateFactory.METRIC_USER_ENABLED_CONF,
      MetricsUserAggregateFactory.DEFAULT_METRIC_USER_ENABLED_CONF)) {
      return;
    }
    int noOfUsers = 10000;
    for (int i = 1; i <= noOfUsers; i++) {
      User.createUserForTesting(conf, "FOO" + i, new String[0]).getUGI()
          .doAs(new PrivilegedAction<Void>() {
            @Override public Void run() {
              rsm.updateGet(tableName, 10);
              return null;
            }
          });
    }
    assertTrue(
        ((MetricsUserAggregateSourceImpl) userAgg.getSource()).getUserSources().size() <= (noOfUsers
            / 10));
    for (int i = 1; i <= noOfUsers / 10; i++) {
      assertFalse(
          HELPER.checkCounterExists("userfoo" + i + "metricgetnumops", userAgg.getSource()));
    }
    HELPER.assertCounter("userfoo" + noOfUsers + "metricgetnumops", 1, userAgg.getSource());
  }
}
