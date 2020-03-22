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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.util.AbstractHBaseTool;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.base.Stopwatch;
import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLine;
import org.apache.hbase.thirdparty.org.apache.commons.cli.Option;

/**
 * Tool to test performance of different {@link org.apache.hadoop.hbase.master.LoadBalancer}
 * implementations.
 * Example command:
 * $ bin/hbase org.apache.hadoop.hbase.master.balancer.LoadBalancerPerformanceEvaluation
 *   -regions 1000 -servers 100
 *   -load_balancer org.apache.hadoop.hbase.master.balancer.SimpleLoadBalancer
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.TOOLS)
public class LoadBalancerPerformanceEvaluation extends AbstractHBaseTool {
  private static final Logger LOG =
      LoggerFactory.getLogger(LoadBalancerPerformanceEvaluation.class.getName());

  protected static final HBaseCommonTestingUtility UTIL = new HBaseCommonTestingUtility();

  private static final int DEFAULT_NUM_REGIONS = 1000000;
  private static Option NUM_REGIONS_OPT = new Option("regions", true,
      "Number of regions to consider by load balancer. Default: " + DEFAULT_NUM_REGIONS);

  private static final int DEFAULT_NUM_SERVERS = 1000;
  private static Option NUM_SERVERS_OPT = new Option("servers", true,
      "Number of servers to consider by load balancer. Default: " + DEFAULT_NUM_SERVERS);

  private static final String DEFAULT_LOAD_BALANCER =
      "org.apache.hadoop.hbase.master.balancer.StochasticLoadBalancer";
  private static Option LOAD_BALANCER_OPT = new Option("load_balancer", true,
      "Type of Load Balancer to use. Default: " + DEFAULT_LOAD_BALANCER);

  private int numRegions;
  private int numServers;
  private String loadBalancerType;
  private Class<?> loadBalancerClazz;

  private LoadBalancer loadBalancer;

  // data
  private List<ServerName> servers;
  private List<RegionInfo> regions;
  private Map<RegionInfo, ServerName> regionServerMap;
  private Map<ServerName, List<RegionInfo>> serverRegionMap;

  // Non-default configurations.
  private void setupConf() {
    conf.setClass(HConstants.HBASE_MASTER_LOADBALANCER_CLASS, loadBalancerClazz, LoadBalancer.class);
    loadBalancer = LoadBalancerFactory.getLoadBalancer(conf);
  }

  private void generateRegionsAndServers() {
    // regions
    regions = new ArrayList<>(numRegions);
    regionServerMap = new HashMap<>(numRegions);
    for (int i = 0; i < numRegions; ++i) {
      byte[] start = new byte[16];
      byte[] end = new byte[16];

      Bytes.putInt(start, 0, i);
      Bytes.putInt(end, 0, i + 1);
      TableName tableName = TableName.valueOf("LoadBalancerPerfTable");
      RegionInfo hri = RegionInfoBuilder.newBuilder(tableName)
        .setStartKey(start)
        .setEndKey(end)
        .setSplit(false)
        .setRegionId(i)
        .build();
      regions.add(hri);
      regionServerMap.put(hri, null);
    }

    // servers
    servers = new ArrayList<>(numServers);
    serverRegionMap = new HashMap<>(numServers);
    for (int i = 0; i < numServers; ++i) {
      ServerName sn = ServerName.valueOf("srv" + i, HConstants.DEFAULT_REGIONSERVER_PORT, i);
      servers.add(sn);
      serverRegionMap.put(sn, i == 0 ? regions : Collections.emptyList());
    }
  }

  @Override
  protected void addOptions() {
    addOption(NUM_REGIONS_OPT);
    addOption(NUM_SERVERS_OPT);
    addOption(LOAD_BALANCER_OPT);
  }

  @Override
  protected void processOptions(CommandLine cmd) {
    numRegions = getOptionAsInt(cmd, NUM_REGIONS_OPT.getOpt(), DEFAULT_NUM_REGIONS);
    Preconditions.checkArgument(numRegions > 0, "Invalid number of regions!");

    numServers = getOptionAsInt(cmd, NUM_SERVERS_OPT.getOpt(), DEFAULT_NUM_SERVERS);
    Preconditions.checkArgument(numServers > 0, "Invalid number of servers!");

    loadBalancerType = cmd.getOptionValue(LOAD_BALANCER_OPT.getOpt(), DEFAULT_LOAD_BALANCER);
    Preconditions.checkArgument(!loadBalancerType.isEmpty(), "Invalid load balancer type!");

    try {
      loadBalancerClazz = Class.forName(loadBalancerType);
    } catch (ClassNotFoundException e) {
      System.err.println("Class '" + loadBalancerType + "' not found!");
      System.exit(1);
    }

    setupConf();
  }

  private String formatResults(final String methodName, final long timeMillis) {
    return String.format("Time for %-25s: %dms%n", methodName, timeMillis);
  }

  @Override
  protected int doWork() throws Exception {
    generateRegionsAndServers();

    String methodName = "roundRobinAssignment";
    LOG.info("Calling " + methodName);
    Stopwatch watch = Stopwatch.createStarted();
    loadBalancer.roundRobinAssignment(regions, servers);
    System.out.print(formatResults(methodName, watch.elapsed(TimeUnit.MILLISECONDS)));

    methodName = "retainAssignment";
    LOG.info("Calling " + methodName);
    watch.reset().start();
    loadBalancer.retainAssignment(regionServerMap, servers);
    System.out.print(formatResults(methodName, watch.elapsed(TimeUnit.MILLISECONDS)));

    methodName = "balanceCluster";
    LOG.info("Calling " + methodName);
    watch.reset().start();

    loadBalancer.balanceTable(HConstants.ENSEMBLE_TABLE_NAME, serverRegionMap);
    System.out.print(formatResults(methodName, watch.elapsed(TimeUnit.MILLISECONDS)));

    return EXIT_SUCCESS;
  }

  public static void main(String[] args) throws IOException {
    LoadBalancerPerformanceEvaluation tool = new LoadBalancerPerformanceEvaluation();
    tool.setConf(UTIL.getConfiguration());
    tool.run(args);
  }
}
