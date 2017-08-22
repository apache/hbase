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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hbase.Clock;
import org.apache.hadoop.hbase.ClockType;
import org.apache.hadoop.hbase.HybridLogicalClock;
import org.apache.hadoop.hbase.SystemClock;
import org.apache.hadoop.hbase.SystemMonotonicClock;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.NodeTime;

import java.util.ArrayList;
import java.util.List;

/**
 * Clocks encapsulates each instance of a {@link Clock} to facilitate doing batch now() or update()
 * on all of the clocks.
 */
public class Clocks {
  private Clock systemClock;
  private Clock systemMonotonicClock;
  private Clock hybridLogicalClock;
  private List<ClockType> clockTypes = new ArrayList<>();

  public Clocks(long maxClockSkew) {
    systemClock = new SystemClock();
    systemMonotonicClock = new SystemMonotonicClock(maxClockSkew);
    hybridLogicalClock = new HybridLogicalClock(maxClockSkew);

    clockTypes.add(systemClock.getClockType());
    clockTypes.add(systemMonotonicClock.getClockType());
    clockTypes.add(hybridLogicalClock.getClockType());
  }

  public long update(ClockType clockType, long timestamp) {
    return getClock(clockType).update(timestamp);
  }

  public long now(ClockType clockType) {
    return getClock(clockType).now();
  }

  public List<Long> updateAll(List<NodeTime> nodeTimes) {
    List<Long> updatedNodeTimes = new ArrayList<>();
    for (NodeTime nodeTime : nodeTimes) {
      ClockType clockType = ProtobufUtil.toClockType(nodeTime.getClockType());
      updatedNodeTimes.add(update(clockType, nodeTime.getTimestamp()));
    }
    return updatedNodeTimes;
  }

  public List<NodeTime> nowAll() {
    List<NodeTime> nodeTimes = new ArrayList<>();
    for (ClockType clockType : clockTypes) {
      NodeTime nodeTime = NodeTime.newBuilder()
          .setClockType(ProtobufUtil.toClockType(clockType))
          .setTimestamp(now(clockType))
          .build();
      nodeTimes.add(nodeTime);
    }
    return nodeTimes;
  }

  public Clock getClock(ClockType clockType) {
    switch (clockType) {
      case HYBRID_LOGICAL:
        return hybridLogicalClock;
      case SYSTEM_MONOTONIC:
        return systemMonotonicClock;
      case SYSTEM:
        return systemClock;
      default:
        throw new IllegalArgumentException("Wrong clock type: " + clockType.toString());
    }
  }

  @VisibleForTesting
  public void setClock(Clock clock) {
    switch (clock.getClockType()) {
      case HYBRID_LOGICAL:
        hybridLogicalClock = clock;
        break;
      case SYSTEM_MONOTONIC:
        systemMonotonicClock = clock;
        break;
      case SYSTEM:
        systemClock = clock;
        break;
      default:
        throw new IllegalArgumentException("Wrong clock type: " + clock.getClockType().toString());
    }
  }
}
