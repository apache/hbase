/*
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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.Action;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MultiRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.RegionAction;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;

/**
 * Tests logging of large batch commands via Multi. Tests are fast, but uses a mini-cluster (to test
 * via "Multi" commands) so classified as MediumTests
 */
@RunWith(Parameterized.class)
@Category(LargeTests.class)
public class TestMultiLogThreshold {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMultiLogThreshold.class);

  private static RSRpcServices SERVICES;

  private static HBaseTestingUtility TEST_UTIL;
  private static Configuration CONF;
  private static final byte[] TEST_FAM = Bytes.toBytes("fam");
  private static RSRpcServices.LogDelegate LD;
  private static HRegionServer RS;
  private static int THRESHOLD;

  @Parameterized.Parameter
  public static boolean rejectLargeBatchOp;

  @Parameterized.Parameters
  public static List<Object[]> params() {
    return Arrays.asList(new Object[] { false }, new Object[] { true });
  }

  @Before
  public void setupTest() throws Exception {
    final TableName tableName = TableName.valueOf("tableName");
    TEST_UTIL = HBaseTestingUtility.createLocalHTU();
    CONF = TEST_UTIL.getConfiguration();
    THRESHOLD = CONF.getInt(HConstants.BATCH_ROWS_THRESHOLD_NAME,
      HConstants.BATCH_ROWS_THRESHOLD_DEFAULT);
    CONF.setBoolean("hbase.rpc.rows.size.threshold.reject", rejectLargeBatchOp);
    TEST_UTIL.startMiniCluster();
    TEST_UTIL.createTable(tableName, TEST_FAM);
    RS = TEST_UTIL.getRSForFirstRegionInTable(tableName);
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  private enum ActionType {
    REGION_ACTIONS, ACTIONS;
  }

  /**
   * Sends a multi request with a certain amount of rows, will populate Multi command with either
   * "rows" number of RegionActions with one Action each or one RegionAction with "rows" number of
   * Actions
   */
  private void sendMultiRequest(int rows, ActionType actionType)
      throws ServiceException, IOException {
    RpcController rpcc = Mockito.mock(HBaseRpcController.class);
    MultiRequest.Builder builder = MultiRequest.newBuilder();
    int numRAs = 1;
    int numAs = 1;
    switch (actionType) {
    case REGION_ACTIONS:
      numRAs = rows;
      break;
    case ACTIONS:
      numAs = rows;
      break;
    }
    for (int i = 0; i < numRAs; i++) {
      RegionAction.Builder rab = RegionAction.newBuilder();
      rab.setRegion(RequestConverter.buildRegionSpecifier(
        HBaseProtos.RegionSpecifier.RegionSpecifierType.REGION_NAME,
        new String("someStuff" + i).getBytes()));
      for (int j = 0; j < numAs; j++) {
        Action.Builder ab = Action.newBuilder();
        rab.addAction(ab.build());
      }
      builder.addRegionAction(rab.build());
    }
    LD = Mockito.mock(RSRpcServices.LogDelegate.class);
    SERVICES = new RSRpcServices(RS, LD);
    SERVICES.multi(rpcc, builder.build());
  }

  @Test
  public void testMultiLogThresholdRegionActions() throws ServiceException, IOException {
    try {
      sendMultiRequest(THRESHOLD + 1, ActionType.REGION_ACTIONS);
      Assert.assertFalse(rejectLargeBatchOp);
    } catch (ServiceException e) {
      Assert.assertTrue(rejectLargeBatchOp);
    }
    Mockito.verify(LD, Mockito.times(1))
      .logBatchWarning(Mockito.anyString(), Mockito.anyInt(), Mockito.anyInt());

    sendMultiRequest(THRESHOLD, ActionType.REGION_ACTIONS);
    Mockito.verify(LD, Mockito.never())
      .logBatchWarning(Mockito.anyString(), Mockito.anyInt(), Mockito.anyInt());

    try {
      sendMultiRequest(THRESHOLD + 1, ActionType.ACTIONS);
      Assert.assertFalse(rejectLargeBatchOp);
    } catch (ServiceException e) {
      Assert.assertTrue(rejectLargeBatchOp);
    }
    Mockito.verify(LD, Mockito.times(1))
      .logBatchWarning(Mockito.anyString(), Mockito.anyInt(), Mockito.anyInt());

    sendMultiRequest(THRESHOLD, ActionType.ACTIONS);
    Mockito.verify(LD, Mockito.never())
      .logBatchWarning(Mockito.anyString(), Mockito.anyInt(), Mockito.anyInt());
  }

}
