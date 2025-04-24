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
package org.apache.hadoop.hbase;

import static org.apache.hadoop.hbase.IntegrationTestingUtility.createPreSplitLoadTestTable;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.StoreEngine;
import org.apache.hadoop.hbase.regionserver.StripeStoreEngine;
import org.apache.hadoop.hbase.testclassification.IntegrationTests;
import org.apache.hadoop.hbase.util.ExitHandler;
import org.apache.hadoop.hbase.util.HFileTestUtil;
import org.apache.hadoop.util.ToolRunner;
import org.junit.experimental.categories.Category;

/**
 * A test class that does the same things as IntegrationTestIngest but with stripe compactions. Can
 * be used with ChaosMonkey in the same manner.
 */
@Category(IntegrationTests.class)
public class IntegrationTestIngestStripeCompactions extends IntegrationTestIngest {
  @Override
  protected void initTable() throws IOException {
    // Do the same as the LoadTestTool does, but with different table configuration.
    TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(getTablename())
      .setValue(StoreEngine.STORE_ENGINE_CLASS_KEY, StripeStoreEngine.class.getName())
      .setValue(HStore.BLOCKING_STOREFILES_KEY, "100").build();
    ColumnFamilyDescriptor familyDescriptor =
      ColumnFamilyDescriptorBuilder.of(HFileTestUtil.DEFAULT_COLUMN_FAMILY);
    ColumnFamilyDescriptor[] columns = new ColumnFamilyDescriptor[] { familyDescriptor };
    createPreSplitLoadTestTable(util.getConfiguration(), tableDescriptor, columns,
      IntegrationTestingUtility.DEFAULT_REGIONS_PER_SERVER);
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    IntegrationTestingUtility.setUseDistributedCluster(conf);
    int ret = ToolRunner.run(conf, new IntegrationTestIngestStripeCompactions(), args);
    ExitHandler.getInstance().exit(ret);
  }
}
