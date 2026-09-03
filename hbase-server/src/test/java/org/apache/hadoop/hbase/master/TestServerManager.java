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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.master.assignment.RegionStates;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(MasterTests.TAG)
@Tag(SmallTests.TAG)
public class TestServerManager {

  private static final class DummyMasterServices extends MockNoopMasterServices {
    private final AssignmentManager am;

    DummyMasterServices(Configuration conf) {
      super(conf);
      am = mock(AssignmentManager.class);
      RegionStates rss = mock(RegionStates.class);
      when(am.getRegionStates()).thenReturn(rss);
    }

    @Override
    public AssignmentManager getAssignmentManager() {
      return am;
    }
  }

  private ServerManager sm;
  private RegionInfo region;

  @BeforeEach
  public void setUp() {
    Configuration conf = HBaseConfiguration.create();
    sm = new ServerManager(new DummyMasterServices(conf), new DummyRegionServerList());
    region = RegionInfoBuilder.newBuilder(TableName.valueOf("t")).build();
  }

  private long lastFlushed(RegionInfo ri) {
    return sm.getLastFlushedSequenceId(ri.getEncodedNameAsBytes()).getLastFlushedSequenceId();
  }

  @Test
  public void testReportRegionOpenSeedsFlushedSequenceId() {
    assertEquals(HConstants.NO_SEQNUM, lastFlushed(region));
    sm.reportRegionOpen(region, 42L);
    assertEquals(42L, lastFlushed(region));
  }

  @Test
  public void testReportRegionOpenDoesNotRegressExistingValue() {
    sm.reportRegionOpen(region, 100L);
    // A later OPEN carrying a smaller openSeqNum (e.g. after a restart replayed less) must not
    // clobber a higher watermark already seeded here or supplied by a heartbeat.
    sm.reportRegionOpen(region, 50L);
    assertEquals(100L, lastFlushed(region));
  }

  @Test
  public void testReportRegionOpenIgnoresNoSeqNum() {
    sm.reportRegionOpen(region, HConstants.NO_SEQNUM);
    assertEquals(HConstants.NO_SEQNUM, lastFlushed(region));
  }

  @Test
  public void testReportRegionOpenIgnoresNegativeSeqNum() {
    sm.reportRegionOpen(region, -5L);
    assertEquals(HConstants.NO_SEQNUM, lastFlushed(region));
  }
}
