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
package org.apache.hadoop.hbase.coprocessor;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.ChunkCreator;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.MemStoreLAB;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.testclassification.CoprocessorTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category({CoprocessorTests.class, SmallTests.class})
public class TestRegionObserverStacking {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRegionObserverStacking.class);

  private static HBaseTestingUtil TEST_UTIL
    = new HBaseTestingUtil();
  static final Path DIR = TEST_UTIL.getDataTestDir();

  public static class ObserverA implements RegionCoprocessor, RegionObserver {
    long id;

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public void postPut(final ObserverContext<RegionCoprocessorEnvironment> c,
        final Put put, final WALEdit edit,
        final Durability durability)
        throws IOException {
      id = EnvironmentEdgeManager.currentTime();
      Threads.sleepWithoutInterrupt(10);
    }
  }

  public static class ObserverB implements RegionCoprocessor, RegionObserver {
    long id;

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public void postPut(final ObserverContext<RegionCoprocessorEnvironment> c,
        final Put put, final WALEdit edit,
        final Durability durability)
        throws IOException {
      id = EnvironmentEdgeManager.currentTime();
      Threads.sleepWithoutInterrupt(10);
    }
  }

  public static class ObserverC implements RegionCoprocessor, RegionObserver {
    long id;

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public void postPut(final ObserverContext<RegionCoprocessorEnvironment> c,
        final Put put, final WALEdit edit,
        final Durability durability)
        throws IOException {
      id = EnvironmentEdgeManager.currentTime();
      Threads.sleepWithoutInterrupt(10);
    }
  }

  HRegion initHRegion(byte[] tableName, String callingMethod, Configuration conf,
    byte[]... families) throws IOException {
    TableDescriptorBuilder builder =
      TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
    for (byte[] family : families) {
      builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(family));
    }
    TableDescriptor tableDescriptor = builder.build();
    ChunkCreator.initialize(MemStoreLAB.CHUNK_SIZE_DEFAULT, false, 0, 0,
      0, null, MemStoreLAB.INDEX_CHUNK_SIZE_PERCENTAGE_DEFAULT);
    RegionInfo info = RegionInfoBuilder.newBuilder(tableDescriptor.getTableName()).build();
    Path path = new Path(DIR + callingMethod);
    HRegion r = HBaseTestingUtil.createRegionAndWAL(info, path, conf, tableDescriptor);
    // this following piece is a hack. currently a coprocessorHost
    // is secretly loaded at OpenRegionHandler. we don't really
    // start a region server here, so just manually create cphost
    // and set it to region.
    RegionCoprocessorHost host = new RegionCoprocessorHost(r,
        Mockito.mock(RegionServerServices.class), conf);
    r.setCoprocessorHost(host);
    return r;
  }

  @Test
  public void testRegionObserverStacking() throws Exception {
    byte[] ROW = Bytes.toBytes("testRow");
    byte[] TABLE = Bytes.toBytes(this.getClass().getSimpleName());
    byte[] A = Bytes.toBytes("A");
    byte[][] FAMILIES = new byte[][] { A } ;

    Configuration conf = TEST_UTIL.getConfiguration();
    HRegion region = initHRegion(TABLE, getClass().getName(),
      conf, FAMILIES);
    RegionCoprocessorHost h = region.getCoprocessorHost();
    h.load(ObserverA.class, Coprocessor.PRIORITY_HIGHEST, conf);
    h.load(ObserverB.class, Coprocessor.PRIORITY_USER, conf);
    h.load(ObserverC.class, Coprocessor.PRIORITY_LOWEST, conf);

    Put put = new Put(ROW);
    put.addColumn(A, A, A);
    region.put(put);

    Coprocessor c = h.findCoprocessor(ObserverA.class.getName());
    long idA = ((ObserverA)c).id;
    c = h.findCoprocessor(ObserverB.class.getName());
    long idB = ((ObserverB)c).id;
    c = h.findCoprocessor(ObserverC.class.getName());
    long idC = ((ObserverC)c).id;

    assertTrue(idA < idB);
    assertTrue(idB < idC);
    HBaseTestingUtil.closeRegionAndWAL(region);
  }
}
