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
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.balancer.replicas.ReplicaKeyCache;
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
 * make it easy to define that system tables will ideally be isolated on their own RegionServer
 * (without needing to manage distinct RegionServer groups).
 */
@InterfaceAudience.Private
final class BalancerConditionals implements Configurable {

  private static final Logger LOG = LoggerFactory.getLogger(BalancerConditionals.class);

  public static final String DISTRIBUTE_REPLICAS_KEY =
    "hbase.master.balancer.stochastic.conditionals.distributeReplicas";
  public static final boolean DISTRIBUTE_REPLICAS_DEFAULT = false;

  public static final String ADDITIONAL_CONDITIONALS_KEY =
    "hbase.master.balancer.stochastic.additionalConditionals";

  private Set<Class<? extends RegionPlanConditional>> conditionalClasses = Collections.emptySet();
  private Set<RegionPlanConditional> conditionals = Collections.emptySet();
  private Configuration conf;

  static BalancerConditionals create() {
    return new BalancerConditionals();
  }

  private BalancerConditionals() {
  }

  boolean shouldRunBalancer(BalancerClusterState cluster) {
    return isConditionalBalancingEnabled() && conditionals.stream()
      .map(RegionPlanConditional::getCandidateGenerators).flatMap(Collection::stream)
      .map(generator -> generator.getWeight(cluster)).anyMatch(weight -> weight > 0);
  }

  Set<Class<? extends RegionPlanConditional>> getConditionalClasses() {
    return Set.copyOf(conditionalClasses);
  }

  Collection<RegionPlanConditional> getConditionals() {
    return conditionals;
  }

  boolean isReplicaDistributionEnabled() {
    return conditionalClasses.stream()
      .anyMatch(DistributeReplicasConditional.class::isAssignableFrom);
  }

  boolean shouldSkipSloppyServerEvaluation() {
    return isConditionalBalancingEnabled();
  }

  boolean isConditionalBalancingEnabled() {
    return !conditionalClasses.isEmpty();
  }

  void clearConditionalWeightCaches() {
    conditionals.stream().map(RegionPlanConditional::getCandidateGenerators)
      .flatMap(Collection::stream)
      .forEach(RegionPlanConditionalCandidateGenerator::clearWeightCache);
  }

  void loadClusterState(BalancerClusterState cluster) {
    conditionals = conditionalClasses.stream().map(clazz -> createConditional(clazz, cluster))
      .filter(Objects::nonNull).collect(Collectors.toSet());
  }

  /**
   * Indicates whether the action is good for our conditional compliance.
   * @param cluster The cluster state
   * @param action  The proposed action
   * @return -1 if conditionals improve, 0 if neutral, 1 if conditionals degrade
   */
  int getViolationCountChange(BalancerClusterState cluster, BalanceAction action) {
    // Cluster is in pre-move state, so figure out the proposed violations
    boolean isViolatingPost = isViolating(cluster, action);
    cluster.doAction(action);

    // Cluster is in post-move state, so figure out the original violations
    BalanceAction undoAction = action.undoAction();
    boolean isViolatingPre = isViolating(cluster, undoAction);

    // Reset cluster
    cluster.doAction(undoAction);

    if (isViolatingPre && isViolatingPost) {
      return 0;
    } else if (!isViolatingPre && isViolatingPost) {
      return 1;
    } else {
      return -1;
    }
  }

  /**
   * Check if the proposed action violates conditionals
   * @param cluster The cluster state
   * @param action  The proposed action
   */
  boolean isViolating(BalancerClusterState cluster, BalanceAction action) {
    conditionals.forEach(conditional -> conditional.setClusterState(cluster));
    if (conditionals.isEmpty()) {
      return false;
    }
    List<RegionPlan> regionPlans = action.toRegionPlans(cluster);
    for (RegionPlan regionPlan : regionPlans) {
      if (isViolating(regionPlan)) {
        return true;
      }
    }
    return false;
  }

  private boolean isViolating(RegionPlan regionPlan) {
    for (RegionPlanConditional conditional : conditionals) {
      if (conditional.isViolating(regionPlan)) {
        return true;
      }
    }
    return false;
  }

  private RegionPlanConditional createConditional(Class<? extends RegionPlanConditional> clazz,
    BalancerClusterState cluster) {
    if (cluster == null) {
      cluster = new BalancerClusterState(Collections.emptyMap(), null, null, null, null);
    }
    try {
      Constructor<? extends RegionPlanConditional> ctor =
        clazz.getDeclaredConstructor(BalancerConditionals.class, BalancerClusterState.class);
      return ReflectionUtils.instantiate(clazz.getName(), ctor, this, cluster);
    } catch (NoSuchMethodException e) {
      LOG.warn("Cannot find constructor with Configuration and "
        + "BalancerClusterState parameters for class '{}': {}", clazz.getName(), e.getMessage());
    }
    return null;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    ImmutableSet.Builder<Class<? extends RegionPlanConditional>> conditionalClasses =
      ImmutableSet.builder();

    boolean distributeReplicas =
      conf.getBoolean(DISTRIBUTE_REPLICAS_KEY, DISTRIBUTE_REPLICAS_DEFAULT);
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
    ReplicaKeyCache.getInstance().setConf(conf);
    loadClusterState(null);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}
