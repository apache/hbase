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
package org.apache.hadoop.hbase.rsgroup;

import static org.apache.hadoop.hbase.rsgroup.RSGroupInfo.DEFAULT_GROUP;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.net.Address;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.collect.Maps;
import org.apache.hbase.thirdparty.com.google.common.collect.Queues;
import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

/**
 * Chore that will move servers between rsgroups to keep the required number of servers in rsgroup
 * automatically, only {@link RSGroupConfigurationAccessor#REQUIRED_SERVER_NUM} needs to be
 * configure in configuration of {@link RSGroupInfo}.
 */
@InterfaceAudience.Private
public class RSGroupAutoScaleChore extends ScheduledChore {

  private static final Logger LOG = LoggerFactory.getLogger(RSGroupAutoScaleChore.class);

  public static final String AUTO_SCALE_ENABLED_CONF_KEY = "hbase.rsgroup.autoscale.enabled";

  public static final String CHORE_PERIOD_CONF_KEY = "hbase.rsgroup.autoscale.chore.period";
  public static final int DEFAULT_CHORE_PERIOD =
      (int) TimeUnit.MINUTES.toMillis(5); // 5 minutes in millis

  public static final String CONSECUTIVE_TIMES_CONF_KEY =
      "hbase.rsgroup.autoscale.chore.consecutive.times";
  public static final Integer CONSECUTIVE_TIMES_DEFAULT = 3;

  // The minimum number of servers to keep in default group.
  public static final String MIN_NUM_OF_SERVER_IN_DEFAULT_GROUP_CONF_KEY =
      "hbase.rsgroup.autoscale.defaultgroup.min.server.num";

  // The minimum ratio of servers to keep in default group.
  public static final String MIN_RATIO_OF_SERVER_IN_DEFAULT_GROUP_CONF_KEY =
      "hbase.rsgroup.autoscale.defaultgroup.min.server.ratio";

  private Configuration conf;

  private HMaster master;

  private RSGroupInfoManager rsGroupInfoManager;

  private int maxConsecutiveHitTimes;

  private Map<String, Integer> consecutiveHitGroups = Maps.newHashMap();

  public RSGroupAutoScaleChore(HMaster master) {
    super(master.getServerName() + "-RSGroupAutoScaleChore", master,
        master.getConfiguration().getInt(CHORE_PERIOD_CONF_KEY, DEFAULT_CHORE_PERIOD));
    this.conf = master.getConfiguration();
    maxConsecutiveHitTimes = conf.getInt(CONSECUTIVE_TIMES_CONF_KEY, CONSECUTIVE_TIMES_DEFAULT);
    this.master = master;
    this.rsGroupInfoManager = master.getRSGroupInfoManager();
  }

  @Override
  protected void chore() {
    try {
      List<RSGroupServersInfo> groupsNeedMoveIn = Lists.newArrayList();
      List<RSGroupServersInfo> groupsNeedMoveOut = Lists.newArrayList();
      findUnqualifiedGroups(groupsNeedMoveIn, groupsNeedMoveOut);
      List<MoveServersPlan> plans = generatePlans(groupsNeedMoveIn, groupsNeedMoveOut);
      executeMovingPlans(plans);
    } catch (Throwable t) {
      LOG.error("Auto scale rsgroups failed", t);
    }
  }

  /**
   * Find all user groups that have more than or less than its required num. Selected Groups need
   * to hit multiple times consecutive.
   *
   * @param groupsNeedMoveIn groups that need move servers in
   * @param groupsNeedMoveOut groups that need move servers out
   */
  private void findUnqualifiedGroups(List<RSGroupServersInfo> groupsNeedMoveIn,
      List<RSGroupServersInfo> groupsNeedMoveOut) throws IOException {
    Set<Address> onlineServers = getOnlineServers();

    rsGroupInfoManager.listRSGroups().stream()
      .filter(group -> !DEFAULT_GROUP.equals(group.getName()))
      .forEach(group -> {
        RSGroupConfigurationAccessor.getRequiredServerNum(group).ifPresent(requiredServerNum -> {
          Set<Address> serversInGroup = Sets.newHashSet(group.getServers());
          serversInGroup.retainAll(onlineServers);
          RSGroupServersInfo groupServersInfo =
            new RSGroupServersInfo(group, serversInGroup, requiredServerNum);
          if (groupServersInfo.needMoveOut()) {
            groupsNeedMoveOut.add(groupServersInfo);
          }
          if (groupServersInfo.needMoveIn()) {
            groupsNeedMoveIn.add(groupServersInfo);
          }
        });
      });

    Set<String> groupNames = Stream.concat(groupsNeedMoveOut.stream(), groupsNeedMoveIn.stream())
        .map(p -> p.rsGroupInfo.getName()).collect(Collectors.toSet());
    consecutiveHitGroups.entrySet().removeIf(e -> !groupNames.contains(e.getKey()));
    groupNames.forEach(group ->
        consecutiveHitGroups.compute(group, (k, v) -> null == v ? 1 : v + 1));

    filterConsecutiveHitGroups(groupsNeedMoveIn);
    filterConsecutiveHitGroups(groupsNeedMoveOut);
    groupsNeedMoveIn.forEach(info -> LOG.info("rsgroup {} has {} servers less than required {}",
        info.rsGroupInfo.getName(), info.numToMoveIn(), info.requiredNum));
    groupsNeedMoveOut.forEach(info -> LOG.info("rsgroup {} has {} servers more than required {}",
        info.rsGroupInfo.getName(), info.numToMoveOut(), info.requiredNum));
  }

  private void filterConsecutiveHitGroups(List<RSGroupServersInfo> infos) {
    infos.removeIf(info ->
        consecutiveHitGroups.get(info.rsGroupInfo.getName()) < maxConsecutiveHitTimes);
  }

  /**
   * Generate plans for moving servers based on groupsNeedMoveIn and groupsNeedMoveOut
   * @param groupsNeedMoveIn groups that need move servers in
   * @param groupsNeedMoveOut groups that need move servers out
   * @return plans need to execute
   */
  private List<MoveServersPlan> generatePlans(List<RSGroupServersInfo> groupsNeedMoveIn,
      List<RSGroupServersInfo> groupsNeedMoveOut) throws IOException {
    Queue<RSGroupServersInfo> moveInQueue = Queues.newArrayDeque(groupsNeedMoveIn);
    Queue<RSGroupServersInfo> moveOutQueue = Queues.newArrayDeque(groupsNeedMoveOut);
    List<MoveServersPlan> plans = Lists.newArrayList();
    // 1. Move servers between moveInQueue and moveOutQueue.
    while (!moveInQueue.isEmpty() && !moveOutQueue.isEmpty()) {
      RSGroupServersInfo in = moveInQueue.peek();
      RSGroupServersInfo out = moveOutQueue.peek();
      // Peek a group from moveInQueue and moveOutQueue respectively.
      // Moving servers from group having more than required to group having less than required
      int num = Math.min(out.numToMoveOut(), in.numToMoveIn());
      Set<Address> movingServers = selectServersRandomly(out.onlineServers, num);
      plans.add(new MoveServersPlan(out.rsGroupInfo, in.rsGroupInfo, movingServers));
      in.onlineServers.addAll(movingServers);
      out.onlineServers.removeAll(movingServers);
      // Remove group from queues once the number of group reaches the required num.
      if (!in.needMoveIn()) {
        moveInQueue.remove();
      }
      if (!out.needMoveOut()) {
        moveOutQueue.remove();
      }
    }

    // 2. Go here, at most one queue is not empty.
    // Next need to move servers from/to default group.
    RSGroupInfo defaultGroup = rsGroupInfoManager.getRSGroup(DEFAULT_GROUP);
    Set<Address> serversInDefault = Sets.newHashSet(defaultGroup.getServers());
    serversInDefault.retainAll(getOnlineServers());
    // If moveOutQueue is not empty, moving servers to default group from groups in moveOutQueue.
    for (RSGroupServersInfo out : moveOutQueue) {
      Set<Address> movingServers = selectServersRandomly(out.onlineServers, out.numToMoveOut());
      plans.add(new MoveServersPlan(out.rsGroupInfo, defaultGroup, movingServers));
      serversInDefault.addAll(movingServers);
      out.onlineServers.removeAll(movingServers);
    }

    // If moveInQueue is not empty, moving servers from default group to groups in moveInQueue.
    int minNumOfServersInDefaultGroup = getMinNumOfServersInDefaultGroup();
    for (RSGroupServersInfo in : moveInQueue) {
      // Ensure that at least minNumOfServersInDefaultGroup servers are in default group.
      if (serversInDefault.size() <= minNumOfServersInDefaultGroup) {
        break;
      }
      int num = Math.min(in.numToMoveIn(), serversInDefault.size() - minNumOfServersInDefaultGroup);
      Set<Address> movingServers = selectServersRandomly(serversInDefault, num);
      plans.add(new MoveServersPlan(defaultGroup, in.rsGroupInfo, movingServers));
      serversInDefault.removeAll(movingServers);
      in.onlineServers.addAll(movingServers);
    }
    return plans;
  }

  private void executeMovingPlans(List<MoveServersPlan> plans) {
    for (MoveServersPlan plan : plans) {
      try {
        rsGroupInfoManager.moveServers(plan.servers, plan.dst.getName());
        LOG.info("Moved {} servers from {} to {}", plan.servers.size(), plan.src.getName(),
            plan.dst.getName());
      } catch (IOException e) {
        LOG.warn("Moving {} servers from {} to {} failed", plan.servers.size(), plan.src.getName(),
          plan.dst.getName(), e);
      }
    }
  }

  private Set<Address> selectServersRandomly(Set<Address> servers, int num) {
    List<Address> serverList = Lists.newArrayList(servers);
    Collections.shuffle(serverList, ThreadLocalRandom.current());
    return Sets.newHashSet(serverList.subList(0, num));
  }

  private Set<Address> getOnlineServers() {
    return master.getServerManager().getOnlineServers().keySet().stream()
        .map(ServerName::getAddress).collect(Collectors.toSet());
  }

  /**
   * Get the minimum number of servers to keep in default group.
   */
  private int getMinNumOfServersInDefaultGroup() {
    int minNum = conf.getInt(MIN_NUM_OF_SERVER_IN_DEFAULT_GROUP_CONF_KEY, 1);
    float minRatio = conf.getFloat(MIN_RATIO_OF_SERVER_IN_DEFAULT_GROUP_CONF_KEY, 0f);
    int size = getOnlineServers().size();
    // 1 at least.
    return Math.max(1, Math.max(minNum, (int)Math.ceil(size * minRatio)));
  }

  private static class RSGroupServersInfo {
    private RSGroupInfo rsGroupInfo;
    private Set<Address> onlineServers;
    private int requiredNum;

    public RSGroupServersInfo(RSGroupInfo rsGroupInfo, Set<Address> onlineServers,
        int requiredNum) {
      this.rsGroupInfo = rsGroupInfo;
      this.onlineServers = onlineServers;
      this.requiredNum = Math.max(1, requiredNum);
    }

    private boolean needMoveIn() {
      return onlineServers.size() < requiredNum;
    }

    private boolean needMoveOut() {
      return onlineServers.size() > requiredNum;
    }

    private int numToMoveIn() {
      return needMoveIn() ? requiredNum - onlineServers.size() : 0;
    }

    private int numToMoveOut() {
      return needMoveOut() ? onlineServers.size() - requiredNum : 0;
    }
  }

  private static class MoveServersPlan {
    private RSGroupInfo src;
    private RSGroupInfo dst;
    private Set<Address> servers;

    public MoveServersPlan(RSGroupInfo src, RSGroupInfo dst, Set<Address> servers) {
      this.src = src;
      this.dst = dst;
      this.servers = servers;
    }
  }

}
