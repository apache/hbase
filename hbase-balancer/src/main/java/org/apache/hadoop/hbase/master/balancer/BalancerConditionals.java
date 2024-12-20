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

import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableSet;

/**
 * Balancer conditionals supplement cost functions in the {@link StochasticLoadBalancer}. Cost
 * functions are insufficient and difficult to work with when making discrete decisions; this is
 * because they operate on a continuous scale, and each cost function's multiplier affects the
 * relative importance of every other cost function. So it is difficult to meaningfully and clearly
 * value many aspects of your region distribution via cost functions alone. Conditionals allow you
 * to very clearly define discrete rules that your balancer would ideally follow. To clarify, a
 * conditional violation will not block a region assignment because we would prefer to have uptime
 * than have perfectly intentional balance. But conditionals allow you to, for example, define that
 * a region's primary and secondary should not live on the same rack. Another example, conditionals
 * make it easy to define that system tables will ideally be isolated on their own RegionServer.
 */
@InterfaceAudience.Private
public final class BalancerConditionals {

  private static final Logger LOG = LoggerFactory.getLogger(BalancerConditionals.class);

  static final BalancerConditionals INSTANCE = new BalancerConditionals();
  public static final String ISOLATE_SYSTEM_TABLES_KEY =
    "hbase.master.balancer.stochastic.conditionals.isolateSystemTables";
  public static final boolean ISOLATE_SYSTEM_TABLES_DEFAULT = false;

  public static final String ISOLATE_META_TABLE_KEY =
    "hbase.master.balancer.stochastic.conditionals.isolateMetaTable";
  public static final boolean ISOLATE_META_TABLE_DEFAULT = false;

  public static final String DISTRIBUTE_REPLICAS_CONDITIONALS_KEY =
    "hbase.master.balancer.stochastic.conditionals.distributeReplicas";
  public static final boolean DISTRIBUTE_REPLICAS_CONDITIONALS_DEFAULT = false;

  public static final String ADDITIONAL_CONDITIONALS_KEY =
    "hbase.master.balancer.stochastic.additionalConditionals";

  private Set<Class<? extends RegionPlanConditional>> conditionalClasses = Collections.emptySet();
  private Set<RegionPlanConditional> conditionals = Collections.emptySet();
  private Configuration conf;

  private BalancerConditionals() {
  }

  boolean shouldRunBalancer(BalancerClusterState cluster) {
    return isConditionalBalancingEnabled() && conditionals.stream()
      .map(RegionPlanConditional::getCandidateGenerator).flatMap(Optional::stream)
      .map(generator -> generator.getWeight(cluster)).anyMatch(weight -> weight > 0);
  }

  Set<Class<? extends RegionPlanConditional>> getConditionalClasses() {
    return Set.copyOf(conditionalClasses);
  }

  Collection<RegionPlanConditional> getConditionals() {
    return conditionals;
  }

  boolean isMetaTableIsolationEnabled() {
    return conditionalClasses.contains(MetaTableIsolationConditional.class);
  }

  boolean isSystemTableIsolationEnabled() {
    return conditionalClasses.contains(SystemTableIsolationConditional.class);
  }

  boolean shouldSkipSloppyServerEvaluation() {
    return conditionals.stream()
      .anyMatch(conditional -> conditional instanceof SystemTableIsolationConditional
        || conditional instanceof MetaTableIsolationConditional);
  }

  void loadConf(Configuration conf) {
    this.conf = conf;
    ImmutableSet.Builder<Class<? extends RegionPlanConditional>> conditionalClasses =
      ImmutableSet.builder();

    boolean isolateSystemTables =
      conf.getBoolean(ISOLATE_SYSTEM_TABLES_KEY, ISOLATE_SYSTEM_TABLES_DEFAULT);
    if (isolateSystemTables) {
      conditionalClasses.add(SystemTableIsolationConditional.class);
    }

    boolean isolateMetaTable = conf.getBoolean(ISOLATE_META_TABLE_KEY, ISOLATE_META_TABLE_DEFAULT);
    if (isolateMetaTable) {
      conditionalClasses.add(MetaTableIsolationConditional.class);
    }

    boolean distributeReplicas = conf.getBoolean(DISTRIBUTE_REPLICAS_CONDITIONALS_KEY,
      DISTRIBUTE_REPLICAS_CONDITIONALS_DEFAULT);
    if (distributeReplicas) {
      conditionalClasses.add(DistributeReplicasConditional.class);
    }

    Class<?>[] classes = conf.getClasses(ADDITIONAL_CONDITIONALS_KEY);
    for (Class<?> clazz : classes) {
      if (!RegionPlanConditional.class.isAssignableFrom(clazz)) {
        LOG.warn("Class {} is not a RegionPlanConditional", clazz.getName());
        continue;
      }
      conditionalClasses.add(clazz.asSubclass(RegionPlanConditional.class));
    }
    this.conditionalClasses = conditionalClasses.build();
    loadClusterState(null);
  }

  void loadClusterState(BalancerClusterState cluster) {
    conditionals = conditionalClasses.stream().map(clazz -> createConditional(clazz, conf, cluster))
      .collect(Collectors.toSet());
  }

  int getConditionalViolationChange(List<RegionPlan> regionPlans) {
    if (conditionals.isEmpty()) {
      return 0;
    }
    int conditionalViolationChange = 0;
    for (RegionPlan regionPlan : regionPlans) {
      conditionalViolationChange += getConditionalViolationChange(conditionals, regionPlan);
    }
    return conditionalViolationChange;
  }

  private static int getConditionalViolationChange(Set<RegionPlanConditional> conditionals,
    RegionPlan regionPlan) {
    RegionPlan inverseRegionPlan = new RegionPlan(regionPlan.getRegionInfo(),
      regionPlan.getDestination(), regionPlan.getSource());
    int currentConditionalViolationCount =
      getConditionalViolationCount(conditionals, inverseRegionPlan);
    int newConditionalViolationCount = getConditionalViolationCount(conditionals, regionPlan);
    int violationChange = newConditionalViolationCount - currentConditionalViolationCount;
    if (violationChange < 0) {
      LOG.trace("Should move region {}_{} from {} to {}", regionPlan.getRegionName(),
        regionPlan.getRegionInfo().getReplicaId(), regionPlan.getSource().getServerName(),
        regionPlan.getDestination().getServerName());
    }
    return violationChange;
  }

  private static int getConditionalViolationCount(Set<RegionPlanConditional> conditionals,
    RegionPlan regionPlan) {
    int regionPlanConditionalViolationCount = 0;
    for (RegionPlanConditional regionPlanConditional : conditionals) {
      if (regionPlanConditional.isViolating(regionPlan)) {
        regionPlanConditionalViolationCount++;
      }
    }
    return regionPlanConditionalViolationCount;
  }

  private RegionPlanConditional createConditional(Class<? extends RegionPlanConditional> clazz,
    Configuration conf, BalancerClusterState cluster) {
    if (conf == null) {
      conf = new Configuration();
    }
    if (cluster == null) {
      cluster = new BalancerClusterState(Collections.emptyMap(), null, null, null, null);
    }
    try {
      Constructor<? extends RegionPlanConditional> ctor =
        clazz.getDeclaredConstructor(Configuration.class, BalancerClusterState.class);
      return ReflectionUtils.instantiate(clazz.getName(), ctor, conf, cluster);
    } catch (NoSuchMethodException e) {
      LOG.warn("Cannot find constructor with Configuration and "
        + "BalancerClusterState parameters for class '{}': {}", clazz.getName(), e.getMessage());
    }
    return null;
  }

  private boolean isConditionalBalancingEnabled() {
    return !conditionalClasses.isEmpty();
  }
}
