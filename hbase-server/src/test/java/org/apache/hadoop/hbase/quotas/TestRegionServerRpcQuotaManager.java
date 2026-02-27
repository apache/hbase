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
package org.apache.hadoop.hbase.quotas;

import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.regionserver.MetricsRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;

/**
 * Test class for {@link RegionServerRpcQuotaManager}.
 */
@Category(SmallTests.class)
public class TestRegionServerRpcQuotaManager {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionServerRpcQuotaManager.class);

  private static void setQuotaCache(RegionServerRpcQuotaManager manager, QuotaCache quotaCache)
    throws Exception {
    Field field = RegionServerRpcQuotaManager.class.getDeclaredField("quotaCache");
    field.setAccessible(true);
    field.set(manager, quotaCache);
  }

  @Test
  public void testCheckScanQuotaNullQuotaCacheNpe() throws Exception {
    RegionServerServices rss = mock(RegionServerServices.class);
    Configuration conf = new Configuration();
    when(rss.getConfiguration()).thenReturn(conf);

    ZKWatcher zk = mock(ZKWatcher.class);
    when(zk.getZNodePaths()).thenReturn(new ZNodePaths(conf));
    when(rss.getZooKeeper()).thenReturn(zk);

    MetricsRegionServer metrics = mock(MetricsRegionServer.class);
    when(rss.getMetrics()).thenReturn(metrics);

    RegionServerRpcQuotaManager manager = spy(new RegionServerRpcQuotaManager(rss));

    setQuotaCache(manager, null);

    OperationQuota throttlingQuota = mock(OperationQuota.class);
    RpcThrottlingException ex =
      new RpcThrottlingException(RpcThrottlingException.Type.NumRequestsExceeded, 1L, "Injected");
    doThrow(ex).when(throttlingQuota).checkScanQuota(any(), anyLong(), anyLong(), anyLong());
    doReturn(throttlingQuota).when(manager).getQuota(any(), any(), anyInt());

    Region region = mock(Region.class);
    TableDescriptor td = mock(TableDescriptor.class);
    TableName table = TableName.valueOf("t");
    when(region.getTableDescriptor()).thenReturn(td);
    when(td.getTableName()).thenReturn(table);
    when(region.getMinBlockSizeBytes()).thenReturn(1);

    ClientProtos.ScanRequest request =
      ClientProtos.ScanRequest.newBuilder().setScannerId(1L).build();

    assertThrows(RpcThrottlingException.class,
      () -> manager.checkScanQuota(region, request, 10L, 20L, 0L));
  }

  @Test
  public void testCheckBatchQuotaNullQuotaCacheNpe() throws Exception {
    RegionServerServices rss = mock(RegionServerServices.class);
    Configuration conf = new Configuration();
    when(rss.getConfiguration()).thenReturn(conf);

    ZKWatcher zk = mock(ZKWatcher.class);
    when(zk.getZNodePaths()).thenReturn(new ZNodePaths(conf));
    when(rss.getZooKeeper()).thenReturn(zk);

    MetricsRegionServer metrics = mock(MetricsRegionServer.class);
    when(rss.getMetrics()).thenReturn(metrics);

    RegionServerRpcQuotaManager manager = spy(new RegionServerRpcQuotaManager(rss));

    setQuotaCache(manager, null);

    OperationQuota throttlingQuota = mock(OperationQuota.class);
    RpcThrottlingException ex =
      new RpcThrottlingException(RpcThrottlingException.Type.NumRequestsExceeded, 1L, "Injected");
    doThrow(ex).when(throttlingQuota).checkBatchQuota(anyInt(), anyInt(), anyBoolean());
    doReturn(throttlingQuota).when(manager).getQuota(any(), any(), anyInt());

    Region region = mock(Region.class);
    TableDescriptor td = mock(TableDescriptor.class);
    TableName table = TableName.valueOf("t");
    when(region.getTableDescriptor()).thenReturn(td);
    when(td.getTableName()).thenReturn(table);
    when(region.getMinBlockSizeBytes()).thenReturn(1);

    assertThrows(RpcThrottlingException.class, () -> manager.checkBatchQuota(region, 1, 0, false));
  }
}
