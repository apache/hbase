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
package org.apache.hadoop.hbase.coprocessor;

import static org.apache.hadoop.hbase.coprocessor.CoprocessorHost.COPROCESSORS_ENABLED_CONF_KEY;
import static org.apache.hadoop.hbase.coprocessor.CoprocessorHost.REGION_COPROCESSOR_CONF_KEY;
import static org.apache.hadoop.hbase.coprocessor.CoprocessorHost.SKIP_LOAD_DUPLICATE_TABLE_COPROCESSOR;
import static org.apache.hadoop.hbase.coprocessor.CoprocessorHost.USER_COPROCESSORS_ENABLED_CONF_KEY;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({SmallTests.class})
public class TestRegionCoprocessorHost {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRegionCoprocessorHost.class);

  @Test
  public void testLoadDuplicateCoprocessor() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.setBoolean(COPROCESSORS_ENABLED_CONF_KEY, true);
    conf.setBoolean(USER_COPROCESSORS_ENABLED_CONF_KEY, true);
    conf.setBoolean(SKIP_LOAD_DUPLICATE_TABLE_COPROCESSOR, true);
    conf.set(REGION_COPROCESSOR_CONF_KEY, SimpleRegionObserver.class.getName());
    TableName tableName = TableName.valueOf("testDoubleLoadingCoprocessor");
    RegionInfo regionInfo = RegionInfoBuilder.newBuilder(tableName).build();
    // config a same coprocessor with system coprocessor
    TableDescriptor tableDesc = TableDescriptorBuilder.newBuilder(tableName)
        .setCoprocessor(SimpleRegionObserver.class.getName()).build();
    HRegion region = mock(HRegion.class);
    when(region.getRegionInfo()).thenReturn(regionInfo);
    when(region.getTableDescriptor()).thenReturn(tableDesc);
    RegionServerServices rsServices = mock(RegionServerServices.class);
    RegionCoprocessorHost host = new RegionCoprocessorHost(region, rsServices, conf);
    // Only one coprocessor SimpleRegionObserver loaded
    assertEquals(1, host.coprocEnvironments.size());

    // Allow to load duplicate coprocessor
    conf.setBoolean(SKIP_LOAD_DUPLICATE_TABLE_COPROCESSOR, false);
    host = new RegionCoprocessorHost(region, rsServices, conf);
    // Two duplicate coprocessors loaded
    assertEquals(2, host.coprocEnvironments.size());
  }
}