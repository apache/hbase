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
package org.apache.hadoop.hbase.coprocessor.example;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.example.RefreshHFilesClient;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.wal.WAL;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestRefreshHFilesEndpoint extends TestRefreshHFilesBase {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRefreshHFilesEndpoint.class);

  @Test
  public void testRefreshRegionHFilesEndpoint() throws Exception {
    setUp(HRegion.class.getName());
    addHFilesToRegions();
    assertEquals(2, HTU.getNumHFiles(TABLE_NAME, FAMILY));
    callRefreshRegionHFilesEndPoint();
    assertEquals(4, HTU.getNumHFiles(TABLE_NAME, FAMILY));
  }

  @Test(expected = IOException.class)
  public void testRefreshRegionHFilesEndpointWithException() throws IOException {
    setUp(HRegionForRefreshHFilesEP.class.getName());
    callRefreshRegionHFilesEndPoint();
  }

  private void callRefreshRegionHFilesEndPoint() throws IOException {
    try {
      RefreshHFilesClient refreshHFilesClient = new RefreshHFilesClient(CONF);
      refreshHFilesClient.refreshHFiles(TABLE_NAME);
    } catch (RetriesExhaustedException rex) {
      if (rex.getCause() instanceof IOException) {
        throw new IOException();
      }
    } catch (Throwable ex) {
      LOG.error(ex.toString(), ex);
      fail("Couldn't call the RefreshRegionHFilesEndpoint");
    }
  }

  public static class HRegionForRefreshHFilesEP extends HRegion {
    HStoreWithFaultyRefreshHFilesAPI store;

    public HRegionForRefreshHFilesEP(final Path tableDir, final WAL wal, final FileSystem fs,
        final Configuration confParam, final RegionInfo regionInfo, final TableDescriptor htd,
        final RegionServerServices rsServices) {
      super(tableDir, wal, fs, confParam, regionInfo, htd, rsServices);
    }

    @Override
    public List<HStore> getStores() {
      List<HStore> list = new ArrayList<>(stores.size());
      /*
       * This is used to trigger the custom definition (faulty)
       * of refresh HFiles API.
       */
      try {
        if (this.store == null) {
          store = new HStoreWithFaultyRefreshHFilesAPI(this,
              ColumnFamilyDescriptorBuilder.of(FAMILY), this.conf);
        }
        list.add(store);
      } catch (IOException ioe) {
        LOG.info("Couldn't instantiate custom store implementation", ioe);
      }

      list.addAll(stores.values());
      return list;
    }
  }

  public static class HStoreWithFaultyRefreshHFilesAPI extends HStore {
    public HStoreWithFaultyRefreshHFilesAPI(final HRegion region,
        final ColumnFamilyDescriptor family, final Configuration confParam) throws IOException {
      super(region, family, confParam, false);
    }

    @Override
    public void refreshStoreFiles() throws IOException {
      throw new IOException();
    }
  }
}
