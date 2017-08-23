/**
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
package org.apache.hadoop.hbase.regionserver;

import static org.mockito.Mockito.verify;

import java.io.IOException;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Action;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MultiRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.RegionAction;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Tests logging of large batch commands via Multi. Tests are fast, but uses a mini-cluster (to test
 * via "Multi" commands) so classified as MediumTests
 */
@Category(MediumTests.class)
public class TestMultiLogThreshold {

  private static RSRpcServices SERVICES;

  private static HBaseTestingUtility TEST_UTIL;
  private static Configuration CONF;
  private static final byte[] TEST_FAM = Bytes.toBytes("fam");
  private static RSRpcServices.LogDelegate LD;
  private static HRegionServer RS;
  private static int THRESHOLD;

  @BeforeClass
  public static void setup() throws Exception {
    final TableName tableName = TableName.valueOf("tableName");
    TEST_UTIL = HBaseTestingUtility.createLocalHTU();
    CONF = TEST_UTIL.getConfiguration();
    THRESHOLD = CONF.getInt(RSRpcServices.BATCH_ROWS_THRESHOLD_NAME,
      RSRpcServices.BATCH_ROWS_THRESHOLD_DEFAULT);
    TEST_UTIL.startMiniCluster();
    TEST_UTIL.createTable(tableName, TEST_FAM);
    RS = TEST_UTIL.getRSForFirstRegionInTable(tableName);
  }

  @Before
  public void setupTest() throws Exception {
    LD = Mockito.mock(RSRpcServices.LogDelegate.class);
    SERVICES = new RSRpcServices(RS, LD);
  }

  private enum ActionType {
    REGION_ACTIONS, ACTIONS;
  }

  /**
   * Sends a multi request with a certain amount of rows, will populate Multi command with either
   * "rows" number of RegionActions with one Action each or one RegionAction with "rows" number of
   * Actions
   */
  private void sendMultiRequest(int rows, ActionType actionType) throws ServiceException {
    RpcController rpcc = Mockito.mock(RpcController.class);
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
    try {
      SERVICES.multi(rpcc, builder.build());
    } catch (ClassCastException e) {
      // swallow expected exception due to mocked RpcController
    }
  }

  @Test
  public void testMultiLogThresholdRegionActions() throws ServiceException, IOException {
    sendMultiRequest(THRESHOLD + 1, ActionType.REGION_ACTIONS);
    verify(LD, Mockito.times(1)).logBatchWarning(Mockito.anyString(), Mockito.anyInt(), Mockito.anyInt());
  }

  @Test
  public void testMultiNoLogThresholdRegionActions() throws ServiceException, IOException {
    sendMultiRequest(THRESHOLD, ActionType.REGION_ACTIONS);
    verify(LD, Mockito.never()).logBatchWarning(Mockito.anyString(), Mockito.anyInt(), Mockito.anyInt());
  }

  @Test
  public void testMultiLogThresholdActions() throws ServiceException, IOException {
    sendMultiRequest(THRESHOLD + 1, ActionType.ACTIONS);
    verify(LD, Mockito.times(1)).logBatchWarning(Mockito.anyString(), Mockito.anyInt(), Mockito.anyInt());
  }

  @Test
  public void testMultiNoLogThresholdAction() throws ServiceException, IOException {
    sendMultiRequest(THRESHOLD, ActionType.ACTIONS);
    verify(LD, Mockito.never()).logBatchWarning(Mockito.anyString(), Mockito.anyInt(), Mockito.anyInt());
  }

}
