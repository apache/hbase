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

package org.apache.hadoop.hbase.master;

import java.util.Map;

import org.apache.hadoop.hbase.metrics.Histogram;
import org.apache.hadoop.hbase.metrics.Snapshot;
import org.apache.hadoop.hbase.util.PairOfSameType;

public class MetricsMasterWrapperStub implements MetricsMasterWrapper {

  @Override public boolean isRunning() {
    return false;
  }

  @Override public String getServerName() {
    return null;
  }

  @Override public double getAverageLoad() {
    return 0;
  }

  @Override public String getClusterId() {
    return null;
  }

  @Override public String getZookeeperQuorum() {
    return null;
  }

  @Override public String[] getCoprocessors() {
    return new String[0];
  }

  @Override public long getStartTime() {
    return 0;
  }

  @Override public long getActiveTime() {
    return 0;
  }

  @Override public boolean getIsActiveMaster() {
    return false;
  }

  @Override public String getRegionServers() {
    return null;
  }

  @Override public int getNumRegionServers() {
    return 0;
  }

  @Override public String getDeadRegionServers() {
    return null;
  }

  @Override public int getNumDeadRegionServers() {
    return 0;
  }

  @Override public long getNumWALFiles() {
    return 0;
  }

  @Override public long getSplitPlanCount() {
    return 0;
  }

  @Override public long getMergePlanCount() {
    return 0;
  }

  @Override public Map<String, Map.Entry<Long, Long>> getTableSpaceUtilization() {
    return null;
  }

  @Override public Map<String, Map.Entry<Long, Long>> getNamespaceSpaceUtilization() {
    return null;
  }

  @Override public long getMasterInitializationTime() {
    return 0;
  }

  @Override public PairOfSameType<Integer> getRegionCounts() {
    return null;
  }

  @Override public long getSplitProcedureRequestCount() {
    return 32;
  }

  @Override public long getSplitProcedureSuccessCount() {
    return 24;
  }

  @Override public Histogram getSplitProcedureTimeHisto() {
    return new Histogram() {
      @Override public void update(int value) {

      }

      @Override public void update(long value) {

      }

      @Override public long getCount() {
        return 24;
      }

      @Override public Snapshot snapshot() {
        return new Snapshot() {
          @Override public long[] getQuantiles(double[] quantiles) {
            return new long[0];
          }

          @Override public long[] getQuantiles() {
            return new long[0];
          }

          @Override public long getCount() {
            return 0;
          }

          @Override public long getCountAtOrBelow(long val) {
            return 0;
          }

          @Override public long get25thPercentile() {
            return 2082;
          }

          @Override public long get75thPercentile() {
            return 2082;
          }

          @Override public long get90thPercentile() {
            return 2082;
          }

          @Override public long get95thPercentile() {
            return 2082;
          }

          @Override public long get98thPercentile() {
            return 2082;
          }

          @Override public long get99thPercentile() {
            return 2082;
          }

          @Override public long get999thPercentile() {
            return 2082;
          }

          @Override public long getMedian() {
            return 2082;
          }

          @Override public long getMax() {
            return 2082;
          }

          @Override public long getMean() {
            return 2082;
          }

          @Override public long getMin() {
            return 2082;
          }
        };
      }
    };
  }
}
