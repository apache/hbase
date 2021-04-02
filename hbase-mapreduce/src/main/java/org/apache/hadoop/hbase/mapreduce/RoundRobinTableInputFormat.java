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
package org.apache.hadoop.hbase.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Process the return from super-class {@link TableInputFormat} (TIF) so as to undo any clumping of
 * {@link InputSplit}s around RegionServers. Spread splits broadly to distribute read-load over
 * RegionServers in the cluster. The super-class TIF returns splits in hbase:meta table order.
 * Adjacent or near-adjacent hbase:meta Regions can be hosted on the same RegionServer -- nothing
 * prevents this. This hbase:maeta ordering of InputSplit placement can be lumpy making it so some
 * RegionServers end up hosting lots of InputSplit scans while contemporaneously other RegionServers
 * host few or none. This class does a pass over the return from the super-class to better spread
 * the load. See the below helpful Flipkart blog post for a description and from where the base of
 * this code comes from (with permission).
 * @see https://tech.flipkart.com/is-data-locality-always-out-of-the-box-in-hadoop-not-really-2ae9c95163cb
 */
@InterfaceAudience.Public
public class RoundRobinTableInputFormat extends TableInputFormat {
  private Boolean hbaseRegionsizecalculatorEnableOriginalValue = null;
  /**
   * Boolean config for whether superclass should produce InputSplits with 'lengths'. If true, TIF
   * will query every RegionServer to get the 'size' of all involved Regions and this 'size' will
   * be used the the InputSplit length. If false, we skip this query and the super-classes
   * returned InputSplits will have lenghths of zero. This override will set the flag to false.
   * All returned lengths will be zero. Makes it so sorting on 'length' becomes a noop. The sort
   * returned by this override will prevail. Thats what we want.
   */
  static String HBASE_REGIONSIZECALCULATOR_ENABLE = "hbase.regionsizecalculator.enable";

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException {
    try {
      // Do a round robin on what we get back from the super-class.
      configure();
      return roundRobin(getSuperSplits(context));
    } finally {
      unconfigure();
    }
  }

  /**
   * Call super-classes' getSplits. Have it out here as its own method so can be overridden.
   */
  List<InputSplit> getSuperSplits(JobContext context) throws IOException {
    return super.getSplits(context);
  }

  /**
   * Spread the splits list so as to avoid clumping on RegionServers. Order splits so every server
   * gets one split before a server gets a second, and so on; i.e. round-robin the splits amongst
   * the servers in the cluster.
   */
  List<InputSplit> roundRobin(List<InputSplit> inputs) throws IOException {
    if ((inputs == null) || inputs.isEmpty()) {
      return inputs;
    }
    List<InputSplit> result = new ArrayList<>(inputs.size());
    // Prepare a hashmap with each region server as key and list of Input Splits as value
    Map<String, List<InputSplit>> regionServerSplits = new HashMap<>();
    for (InputSplit is : inputs) {
      if (is instanceof TableSplit) {
        String regionServer = ((TableSplit) is).getRegionLocation();
        if (regionServer != null && !StringUtils.isBlank(regionServer)) {
          regionServerSplits.computeIfAbsent(regionServer, k -> new ArrayList<>()).add(is);
          continue;
        }
      }
      // If TableSplit or region server not found, add it anyways.
      result.add(is);
    }
    // Write out splits in a manner that spreads splits for a RegionServer to avoid 'clumping'.
    while (!regionServerSplits.isEmpty()) {
      Iterator<List<InputSplit>> iter = regionServerSplits.values().iterator();
      while (iter.hasNext()) {
        List<InputSplit> inputSplitListForRegion = iter.next();
        if (!inputSplitListForRegion.isEmpty()) {
          result.add(inputSplitListForRegion.remove(0));
        }
        if (inputSplitListForRegion.isEmpty()) {
          iter.remove();
        }
      }
    }
    return result;
  }

  /**
   * Adds a configuration to the Context disabling remote rpc'ing to figure Region size
   * when calculating InputSplits. See up in super-class TIF where we rpc to every server to find
   * the size of all involved Regions. Here we disable this super-class action. This means
   * InputSplits will have a length of zero. If all InputSplits have zero-length InputSplits, the
   * ordering done in here will 'pass-through' Hadoop's length-first sort. The superclass TIF will
   * ask every node for the current size of each of the participating Table Regions. It does this
   * because it wants to schedule the biggest Regions first (This fixation comes of hadoop itself
   * -- see JobSubmitter where it sorts inputs by size). This extra diligence takes time and is of
   * no utility in this RRTIF where spread is of more import than size-first. Also, if a rolling
   * restart is happening when we go to launch the job, the job launch may fail because the request
   * for Region size fails -- even after retries -- because rolled RegionServer may take a while to
   * come online: e.g. it takes java 90 seconds to allocate a 160G. RegionServer is offline during
   * this time. The job launch will fail with 'Connection rejected'. So, we set
   * 'hbase.regionsizecalculator.enable' to false here in RRTIF.
   * @see #unconfigure()
   */
  void configure() {
    if (getConf().get(HBASE_REGIONSIZECALCULATOR_ENABLE) != null) {
      this.hbaseRegionsizecalculatorEnableOriginalValue = getConf().
        getBoolean(HBASE_REGIONSIZECALCULATOR_ENABLE, true);
    }
    getConf().setBoolean(HBASE_REGIONSIZECALCULATOR_ENABLE, false);
  }

  /**
   * @see #configure()
   */
  void unconfigure() {
    if (this.hbaseRegionsizecalculatorEnableOriginalValue == null) {
      getConf().unset(HBASE_REGIONSIZECALCULATOR_ENABLE);
    } else {
      getConf().setBoolean(HBASE_REGIONSIZECALCULATOR_ENABLE,
        this.hbaseRegionsizecalculatorEnableOriginalValue);
    }
  }

  /**
   * Pass table name as argument. Set the zk ensemble to use with the System property
   * 'hbase.zookeeper.quorum'
   */
  public static void main(String[] args) throws IOException {
    TableInputFormat tif = new RoundRobinTableInputFormat();
    final Configuration configuration = HBaseConfiguration.create();
    configuration.setBoolean("hbase.regionsizecalculator.enable", false);
    configuration.set(HConstants.ZOOKEEPER_QUORUM,
      System.getProperty(HConstants.ZOOKEEPER_QUORUM, "localhost"));
    configuration.set(TableInputFormat.INPUT_TABLE, args[0]);
    tif.setConf(configuration);
    List<InputSplit> splits = tif.getSplits(new JobContextImpl(configuration, new JobID()));
    for (InputSplit split: splits) {
      System.out.println(split);
    }
  }
}
