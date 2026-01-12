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
package org.apache.hadoop.hbase.io.asyncfs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.asyncfs.monitor.ExcludeDatanodeManager;
import org.apache.hadoop.hbase.io.asyncfs.monitor.StreamSlowMonitor;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(MiscTests.TAG)
@Tag(SmallTests.TAG)
public class TestExcludeDatanodeManager {

  @Test
  public void testExcludeSlowDNBySpeed() {
    Configuration conf = HBaseConfiguration.create();
    ExcludeDatanodeManager excludeDatanodeManager = new ExcludeDatanodeManager(conf);
    StreamSlowMonitor streamSlowDNsMonitor =
      excludeDatanodeManager.getStreamSlowMonitor("testMonitor");
    assertEquals(0, excludeDatanodeManager.getExcludeDNs().size());
    DatanodeInfo datanodeInfo = new DatanodeInfo.DatanodeInfoBuilder().setIpAddr("0.0.0.0")
      .setHostName("hostname1").setDatanodeUuid("uuid1").setXferPort(111).setInfoPort(222)
      .setInfoSecurePort(333).setIpcPort(444).setNetworkLocation("location1").build();
    streamSlowDNsMonitor.checkProcessTimeAndSpeed(datanodeInfo, 100000, 5100,
      System.currentTimeMillis() - 5100, 0);
    streamSlowDNsMonitor.checkProcessTimeAndSpeed(datanodeInfo, 100000, 5100,
      System.currentTimeMillis() - 5100, 0);
    streamSlowDNsMonitor.checkProcessTimeAndSpeed(datanodeInfo, 100000, 5100,
      System.currentTimeMillis() - 5100, 0);
    assertEquals(1, excludeDatanodeManager.getExcludeDNs().size());
    assertTrue(excludeDatanodeManager.getExcludeDNs().containsKey(datanodeInfo));
  }

  @Test
  public void testExcludeSlowDNByProcessTime() {
    Configuration conf = HBaseConfiguration.create();
    ExcludeDatanodeManager excludeDatanodeManager = new ExcludeDatanodeManager(conf);
    StreamSlowMonitor streamSlowDNsMonitor =
      excludeDatanodeManager.getStreamSlowMonitor("testMonitor");
    assertEquals(0, excludeDatanodeManager.getExcludeDNs().size());
    DatanodeInfo datanodeInfo = new DatanodeInfo.DatanodeInfoBuilder().setIpAddr("0.0.0.0")
      .setHostName("hostname1").setDatanodeUuid("uuid1").setXferPort(111).setInfoPort(222)
      .setInfoSecurePort(333).setIpcPort(444).setNetworkLocation("location1").build();
    streamSlowDNsMonitor.checkProcessTimeAndSpeed(datanodeInfo, 5000, 7000,
      System.currentTimeMillis() - 7000, 0);
    streamSlowDNsMonitor.checkProcessTimeAndSpeed(datanodeInfo, 5000, 7000,
      System.currentTimeMillis() - 7000, 0);
    streamSlowDNsMonitor.checkProcessTimeAndSpeed(datanodeInfo, 5000, 7000,
      System.currentTimeMillis() - 7000, 0);
    assertEquals(1, excludeDatanodeManager.getExcludeDNs().size());
    assertTrue(excludeDatanodeManager.getExcludeDNs().containsKey(datanodeInfo));
  }
}
