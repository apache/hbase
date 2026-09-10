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
package org.apache.hadoop.hbase.master.balancer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.AbstractHBaseTool;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(MasterTests.TAG)
@Tag(LargeTests.TAG)
public class TestLoadBalancerPerformanceEvaluation {

  private static final LoadBalancerPerformanceEvaluation tool =
    new LoadBalancerPerformanceEvaluation();

  @BeforeEach
  public void setUpBeforeEach() {
    tool.setConf(HBaseConfiguration.create());
  }

  @AfterEach
  public void tearDownAfterEach() {
    DefaultMetricsSystem.shutdown();
  }

  @Test
  public void testLoadBalancerWithDefaultParams() throws IOException {
    int ret = tool.run(new String[0]);
    assertEquals(AbstractHBaseTool.EXIT_SUCCESS, ret);
  }

  @Test
  public void testStochasticLoadBalancer() throws Exception {
    testLoadBalancer(StochasticLoadBalancer.class);
  }

  @Test
  public void testSimpleLoadBalancer() throws Exception {
    testLoadBalancer(SimpleLoadBalancer.class);
  }

  @Test
  public void testCacheAwareLoadBalancer() throws Exception {
    testLoadBalancer(CacheAwareLoadBalancer.class);
  }

  private void testLoadBalancer(Class<? extends LoadBalancer> loadBalancerClass) throws Exception {
    String[] args =
      { "-regions", "1000", "-servers", "100", "-load_balancer", loadBalancerClass.getName() };
    int ret = tool.run(args);
    assertEquals(AbstractHBaseTool.EXIT_SUCCESS, ret);
  }

  @Test
  public void testInvalidRegions() {
    String[] args = { "-regions", "-100" };
    IllegalArgumentException exception =
      assertThrows(IllegalArgumentException.class, () -> tool.run(args));
    assertEquals("Invalid number of regions!", exception.getMessage());
  }

  @Test
  public void testInvalidServers() {
    String[] args = { "-servers", "0" };
    IllegalArgumentException exception =
      assertThrows(IllegalArgumentException.class, () -> tool.run(args));
    assertEquals("Invalid number of servers!", exception.getMessage());
  }

  @Test
  public void testEmptyLoadBalancer() {
    String[] args = { "-load_balancer", "" };
    IllegalArgumentException exception =
      assertThrows(IllegalArgumentException.class, () -> tool.run(args));
    assertEquals("Invalid load balancer type!", exception.getMessage());
  }
}
