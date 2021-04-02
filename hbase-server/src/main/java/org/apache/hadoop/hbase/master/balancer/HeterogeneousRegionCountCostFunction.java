/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hbase.master.balancer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ServerName;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is an optional Cost function designed to allow region count skew across RegionServers. A
 * rule file is loaded from the local FS or HDFS before balancing. It contains lines of rules. A
 * rule is composed of a regexp for hostname, and a limit. For example, we could have:
 * <p>
 * * rs[0-9] 200 * rs1[0-9] 50
 * </p>
 * RegionServers with hostname matching the first rules will have a limit of 200, and the others 50.
 * If there's no match, a default is set. The costFunction is trying to fill all RegionServers
 * linearly, meaning that if the global usage is at 50%, then all RegionServers should hold half of
 * their capacity in terms of regions. In order to use this CostFunction, you need to set the
 * following options:
 * <ul>
 * <li>hbase.master.balancer.stochastic.additionalCostFunctions</li>
 * <li>hbase.master.balancer.stochastic.heterogeneousRegionCountRulesFile</li>
 * <li>hbase.master.balancer.stochastic.heterogeneousRegionCountDefault</li>
 * </ul>
 * The rule file can be located on local FS or HDFS, depending on the prefix (file//: or hdfs://).
 */
@InterfaceAudience.Private
public class HeterogeneousRegionCountCostFunction extends StochasticLoadBalancer.CostFunction {

  /**
   * configuration used for the path where the rule file is stored.
   */
  static final String HBASE_MASTER_BALANCER_HETEROGENEOUS_RULES_FILE =
      "hbase.master.balancer.heterogeneousRegionCountRulesFile";
  private static final Logger LOG =
      LoggerFactory.getLogger(HeterogeneousRegionCountCostFunction.class);
  /**
   * Default rule to apply when the rule file is not found. Default to 200.
   */
  private static final String HBASE_MASTER_BALANCER_HETEROGENEOUS_RULES_DEFAULT =
      "hbase.master.balancer.heterogeneousRegionCountDefault";
  /**
   * Cost for the function. Default to 500, can be changed.
   */
  private static final String REGION_COUNT_SKEW_COST_KEY =
      "hbase.master.balancer.stochastic.heterogeneousRegionCountCost";
  private static final float DEFAULT_REGION_COUNT_SKEW_COST = 500;
  private final String rulesPath;

  /**
   * Contains the rules, key is the regexp for ServerName, value is the limit
   */
  private final Map<Pattern, Integer> limitPerRule;

  /**
   * This is a cache, used to not go through all the limitPerRule map when searching for limit
   */
  private final Map<ServerName, Integer> limitPerRS;
  private final Configuration conf;
  private int defaultNumberOfRegions;

  /**
   * Total capacity of regions for the cluster, based on the online RS and their associated rules
   */
  private int totalCapacity = 0;
  double overallUsage;

  HeterogeneousRegionCountCostFunction(final Configuration conf) {
    super(conf);
    this.conf = conf;
    this.limitPerRS = new HashMap<>();
    this.limitPerRule = new HashMap<>();
    this.setMultiplier(conf.getFloat(REGION_COUNT_SKEW_COST_KEY, DEFAULT_REGION_COUNT_SKEW_COST));
    this.rulesPath = conf.get(HBASE_MASTER_BALANCER_HETEROGENEOUS_RULES_FILE);
    this.defaultNumberOfRegions =
        conf.getInt(HBASE_MASTER_BALANCER_HETEROGENEOUS_RULES_DEFAULT, 200);

    if (this.defaultNumberOfRegions < 0) {
      LOG.warn("invalid configuration '" + HBASE_MASTER_BALANCER_HETEROGENEOUS_RULES_DEFAULT
          + "'. Setting default to 200");
      this.defaultNumberOfRegions = 200;
    }
    if (conf.getFloat(StochasticLoadBalancer.RegionCountSkewCostFunction.REGION_COUNT_SKEW_COST_KEY,
      StochasticLoadBalancer.RegionCountSkewCostFunction.DEFAULT_REGION_COUNT_SKEW_COST) > 0) {
      LOG.warn("regionCountCost is not set to 0, "
          + " this will interfere with the HeterogeneousRegionCountCostFunction!");
    }
  }

  /**
   * Called once per LB invocation to give the cost function to initialize it's state, and perform
   * any costly calculation.
   */
  @Override
  void init(final BaseLoadBalancer.Cluster cluster) {
    this.cluster = cluster;
    this.loadRules();
  }

  @Override
  protected double cost() {
    double cost = 0;
    final double targetUsage = ((double) this.cluster.numRegions / (double) this.totalCapacity);

    for (int i = 0; i < this.cluster.numServers; i++) {
      // retrieve capacity for each RS
      final ServerName sn = this.cluster.servers[i];
      final double limit = this.limitPerRS.getOrDefault(sn, defaultNumberOfRegions);
      final double nbrRegions = this.cluster.regionsPerServer[i].length;
      final double usage = nbrRegions / limit;
      if (usage > targetUsage) {
        // cost is the number of regions above the local limit
        final double localCost = (nbrRegions - Math.round(limit * targetUsage)) / limit;
        cost += localCost;
      }
    }

    return cost / (double) this.cluster.numServers;
  }

  /**
   * used to load the rule files.
   */
  void loadRules() {
    final List<String> lines = readFile(this.rulesPath);
    if (null == lines) {
      LOG.warn("cannot load rules file, keeping latest rules file which has "
          + this.limitPerRule.size() + " rules");
      return;
    }

    LOG.info("loading rules file '" + this.rulesPath + "'");
    this.limitPerRule.clear();
    for (final String line : lines) {
      try {
        if (line.length() == 0) {
          continue;
        }
        if (line.startsWith("#")) {
          continue;
        }
        final String[] splits = line.split(" ");
        if (splits.length != 2) {
          throw new IOException(
              "line '" + line + "' is malformated, " + "expected [regexp] [limit]. Skipping line");
        }

        final Pattern pattern = Pattern.compile(splits[0]);
        final Integer limit = Integer.parseInt(splits[1]);
        this.limitPerRule.put(pattern, limit);
      } catch (final IOException | NumberFormatException | PatternSyntaxException e) {
        LOG.error("error on line: " + e);
      }
    }
    this.rebuildCache();
  }

  /**
   * used to read the rule files from either HDFS or local FS
   */
  private List<String> readFile(final String filename) {
    if (null == filename) {
      return null;
    }
    try {
      if (filename.startsWith("file:")) {
        return readFileFromLocalFS(filename);
      }
      return readFileFromHDFS(filename);
    } catch (IOException e) {
      LOG.error("cannot read rules file located at ' " + filename + " ':" + e.getMessage());
      return null;
    }
  }

  /**
   * used to read the rule files from HDFS
   */
  private List<String> readFileFromHDFS(final String filename) throws IOException {
    final Path path = new Path(filename);
    final FileSystem fs = FileSystem.get(this.conf);
    final BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
    return readLines(reader);
  }

  /**
   * used to read the rule files from local FS
   */
  private List<String> readFileFromLocalFS(final String filename) throws IOException {
    BufferedReader reader = new BufferedReader(new FileReader(filename));
    return readLines(reader);
  }

  private List<String> readLines(BufferedReader reader) throws IOException {
    final List<String> records = new ArrayList<>();
    try {
      String line;
      while ((line = reader.readLine()) != null) {
        records.add(line);
      }
    } finally {
      reader.close();
    }
    return records;
  }

  /**
   * Rebuild cache matching ServerNames and their capacity.
   */
  private void rebuildCache() {
    LOG.debug("Rebuilding cache of capacity for each RS");
    this.limitPerRS.clear();
    this.totalCapacity = 0;
    if (null == this.cluster) {
      return;
    }
    for (int i = 0; i < this.cluster.numServers; i++) {
      final ServerName sn = this.cluster.servers[i];
      final int capacity = this.findLimitForRS(sn);
      LOG.debug(sn.getHostname() + " can hold " + capacity + " regions");
      this.totalCapacity += capacity;
    }
    overallUsage = (double) this.cluster.numRegions / (double) this.totalCapacity;
    LOG.info("Cluster can hold " + this.cluster.numRegions + "/" + this.totalCapacity + " regions ("
        + Math.round(overallUsage * 100) + "%)");
    if (overallUsage >= 1) {
      LOG.warn("Cluster is overused, {}", overallUsage);
    }
  }

  /**
   * Find the limit for a ServerName. If not found then return the default value
   * @param serverName the server we are looking for
   * @return the limit
   */
  int findLimitForRS(final ServerName serverName) {
    boolean matched = false;
    int limit = -1;
    for (final Map.Entry<Pattern, Integer> entry : this.limitPerRule.entrySet()) {
      if (entry.getKey().matcher(serverName.getHostname()).matches()) {
        matched = true;
        limit = entry.getValue();
        break;
      }
    }
    if (!matched) {
      limit = this.defaultNumberOfRegions;
    }
    // Feeding cache
    this.limitPerRS.put(serverName, limit);
    return limit;
  }

  int getNumberOfRulesLoaded() {
    return this.limitPerRule.size();
  }
}
