/**
 * Copyright The Apache Software Foundation
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
package org.apache.hadoop.hbase.master;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.master.RegionManager.AssignmentLoadBalancer;
import org.apache.hadoop.hbase.master.RegionManager.LoadBalancer;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestRegionPlacement extends RegionPlacementTestBase {

  private final static int SLAVES = 4;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    setUpCluster(SLAVES);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @After
  public void cleanUpTables() throws IOException, InterruptedException {
    cleanUp();
  }

  @Test
  public void testLoadBalancerImpl() throws Exception {
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    HMaster m = cluster.getMaster();
    LoadBalancer loadBalancer = m.getRegionManager().getLoadBalancer();
    // Verify the master runs with the correct load balancer.
    assertTrue(loadBalancer instanceof AssignmentLoadBalancer);
  }

  /**
   * Test whether the regionservers are balanced by the number of primary
   * regions assigned. Create two tables and check whether the primaries are
   * placed like we expected
   *
   * @throws Exception
   */
  @Test(timeout = 360000)
  public void testPrimaryPlacement() throws Exception {
    final String tableName = "testPrimaryPlacement";
    final String tableNameTwo = "testPrimaryPlacement2";

    // Create a table with REGION_NUM regions.
    createTable(tableName, REGION_NUM);

    waitOnTable(tableName);
    waitOnStableRegionMovement();

    AssignmentPlan plan = rp.getExistingAssignmentPlan();

    // NumRegions -> Num Region Server with that num regions.
    Map<Integer, Integer> expected = new HashMap<Integer, Integer>();

    // we expect 2 regionservers with 3 regions and 2 with 2 regions
    expected.put(3, 2);
    expected.put(2, 2);

    waitOnTable(tableName);
    waitOnStableRegionMovement();


    assertTrue(verifyNumPrimaries(expected, plan));

    //create additional table with 5 regions
    createTable(tableNameTwo, 6);
    expected.clear();
    // after this we expect 4 regionservers with 4 regions
    expected.put(4, 4);

    waitOnTable(tableName);
    waitOnTable(tableNameTwo);
    waitOnStableRegionMovement();

    plan = rp.getExistingAssignmentPlan();
    assertTrue(verifyNumPrimaries(expected, plan));
  }

  // Test case1: Verify the region assignment for the exiting table
  // is consistent with the assignment plan and all the region servers get
  // correctly favored nodes updated.
  // Verify all the user region are assigned. REGION_NUM regions are opened
  @Test(timeout = 360000)
  public void testRegionPlacement() throws Exception {
    AssignmentPlan currentPlan;

    // Reset all of the counters.
    resetLastOpenedRegionCount();
    resetLastRegionOnPrimary();

    // Create a table with REGION_NUM regions.
    final String tableName = "testRegionAssignment";
    createTable(tableName, REGION_NUM);

    waitOnTable(tableName);
    waitOnStableRegionMovement();

    verifyRegionMovementNum(REGION_NUM);

    // Get the assignment plan from scanning the META table
    currentPlan = rp.getExistingAssignmentPlan();

    RegionPlacement.printAssignmentPlan(currentPlan);
    // Verify the plan from the META has covered all the user regions
    assertEquals(REGION_NUM, currentPlan.getAssignmentMap().keySet().size());

    // Verify all the user regions are assigned to the primary region server
    // based on the plan
    verifyRegionOnPrimaryRS(REGION_NUM);

    // Verify all the region server are update with the latest favored nodes
    verifyRegionServerUpdated(currentPlan);
    RegionPlacement.printAssignmentPlan(currentPlan);

  }

  // Test Case 2: To verify whether the region placement tools can
  // correctly update the new assignment plan to META and Region Server.
  // The new assignment plan is generated by shuffle the existing assignment
  // plan by switching PRIMARY, SECONDARY and TERTIARY nodes.
  // Shuffle the plan by switching the secondary region server with
  // the tertiary.
  @Test(timeout = 360000)
  public void testRegionPlacementShuffle() throws Exception {
    // Create a table with REGION_NUM regions.
    final String tableName = "testRegionPlacementShuffle";
    createTable(tableName, REGION_NUM);

    AssignmentPlan currentPlan = rp.getExistingAssignmentPlan();

    // Wait on everything to settle down
    waitOnTable(tableName);
    waitOnStableRegionMovement();

    // Reset the counts so that previous tests don't impact this.
    resetLastOpenedRegionCount();
    resetLastRegionOnPrimary();

    // Shuffle the secondary with tertiary favored nodes
    AssignmentPlan shuffledPlan = this.shuffleAssignmentPlan(currentPlan,
        AssignmentPlan.POSITION.SECONDARY, AssignmentPlan.POSITION.TERTIARY);

    // Let the region placement update the META and Region Servers
    rp.updateAssignmentPlan(shuffledPlan);

    // Verify the region assignment. There are supposed to no region reassignment
    // All the regions are still on the primary region region server
    verifyRegionAssignment(shuffledPlan, 0, REGION_NUM);

    // Shuffle the plan by switching the primary with secondary and
    // verify the region reassignment is consistent with the plan.
    shuffledPlan = this.shuffleAssignmentPlan(currentPlan,
        AssignmentPlan.POSITION.PRIMARY, AssignmentPlan.POSITION.SECONDARY);

    // Let the region placement update the META and Region Servers
    rp.updateAssignmentPlan(shuffledPlan);

    // Really really wait for the table.
    waitOnTable(tableName);
    waitOnStableRegionMovement();

    verifyRegionAssignment(shuffledPlan, REGION_NUM, REGION_NUM);
  }

  /**
   * Test to make sure that tables created with a specific server set
   * will remain on those servers though region plan creation and updating.
   *
   * @throws Exception
   */
  @Test(timeout = 180000)
  public void testPinnedTable() throws Exception {
    String tableName = "testPinnedTable";

    resetLastOpenedRegionCount();
    resetLastRegionOnPrimary();

    try {
      MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();

      HTableDescriptor htd = new HTableDescriptor(tableName);
      htd.addFamily(new HColumnDescriptor("d"));

      Set<HServerAddress> servers = new HashSet<HServerAddress>(3);
      HRegionServer unusedServer = cluster.getRegionServer(3);

      for (int i =0 ; i <3; i++) {
        servers.add(cluster.getRegionServer(i).getServerInfo().getServerAddress());
      }

      htd.setServers(servers);
      admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
      admin.createTable(htd, Bytes.toBytes("aaaa"), Bytes.toBytes("zzzz"), REGION_NUM);

      // Wait for things to stabilize
      waitOnTable(tableName);
      waitOnStableRegionMovement();

      // Reset all of the counters.
      resetLastRegionOnPrimary();
      resetLastOpenedRegionCount();

      verifyRegionAssignment(rp.getExistingAssignmentPlan(), 0, REGION_NUM);
      assertPinned(tableName, cluster, servers, unusedServer);

      // Generate and apply a new region plan.
      rp.updateAssignmentPlan(rp.getNewAssignmentPlan());

      // Wait for things to stabilize
      waitOnTable(tableName);
      waitOnStableRegionMovement();

      // Verify current plan
      verifyRegionAssignment(rp.getExistingAssignmentPlan(), 0, REGION_NUM);
      // Make sure the plans are stable.
      verifyRegionAssignment(rp.getNewAssignmentPlan(), 0, REGION_NUM);

      // Assert that the regions are still on the correct servers
      // Though they really should be since there haven't been any region moves.
      assertPinned(tableName, cluster, servers, unusedServer);

    } finally {
      if (admin != null) admin.close();
    }
  }


  /**
   * Used to test the correctness of this class.
   */
  @Test
  public void testRandomizedMatrix() throws Exception {
    int rows = 100;
    int cols = 100;
    float[][] matrix = new float[rows][cols];
    Random random = new Random();
    for (int i = 0; i < rows; i++) {
      for (int j = 0; j < cols; j++) {
        matrix[i][j] = random.nextFloat();
      }
    }

    // Test that inverting a transformed matrix gives the original matrix.
    RegionPlacement.RandomizedMatrix rm =
        new RegionPlacement.RandomizedMatrix(rows, cols);
    float[][] transformed = rm.transform(matrix);
    float[][] invertedTransformed = rm.invert(transformed);
    for (int i = 0; i < rows; i++) {
      for (int j = 0; j < cols; j++) {
        if (matrix[i][j] != invertedTransformed[i][j]) {
          throw new RuntimeException();
        }
      }
    }

    // Test that the indices on a transformed matrix can be inverted to give
    // the same values on the original matrix.
    int[] transformedIndices = new int[rows];
    for (int i = 0; i < rows; i++) {
      transformedIndices[i] = random.nextInt(cols);
    }
    int[] invertedTransformedIndices = rm.invertIndices(transformedIndices);
    float[] transformedValues = new float[rows];
    float[] invertedTransformedValues = new float[rows];
    for (int i = 0; i < rows; i++) {
      transformedValues[i] = transformed[i][transformedIndices[i]];
      invertedTransformedValues[i] = matrix[i][invertedTransformedIndices[i]];
    }
    Arrays.sort(transformedValues);
    Arrays.sort(invertedTransformedValues);
    if (!Arrays.equals(transformedValues, invertedTransformedValues)) {
      throw new RuntimeException();
    }
  }

  /**
   * Download current assignment plan, serialize it to json and deserialize the json.
   * The two plans should be identical.
   */
  @Test
  public void testJsonToAP() throws Exception {
    createTable("testJsonAssignmentPlan", 3);

    AssignmentPlan currentPlan = rp.getExistingAssignmentPlan();
    RegionPlacement.printAssignmentPlan(currentPlan);
    AssignmentPlanData data = AssignmentPlanData.constructFromAssignmentPlan(currentPlan);

    String jsonStr = new ObjectMapper().defaultPrettyPrintingWriter().writeValueAsString(data);
    LOG.info("Json version of current assignment plan: " + jsonStr);
    AssignmentPlan loadedPlan = rp.loadPlansFromJson(jsonStr);
    RegionPlacement.printAssignmentPlan(loadedPlan);
    assertEquals("Loaded plan should be the same with current plan", currentPlan, loadedPlan);

  }
}

