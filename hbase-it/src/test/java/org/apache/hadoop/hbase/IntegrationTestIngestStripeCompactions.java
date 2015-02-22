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

package org.apache.hadoop.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.StoreEngine;
import org.apache.hadoop.hbase.regionserver.StripeStoreEngine;
import org.apache.hadoop.hbase.testclassification.IntegrationTests;
import org.apache.hadoop.hbase.util.LoadTestTool;
import org.junit.experimental.categories.Category;

/**
 * A test class that does the same things as IntegrationTestIngest but with stripe
 * compactions. Can be used with ChaosMonkey in the same manner.
 */
@Category(IntegrationTests.class)
public class IntegrationTestIngestStripeCompactions extends IntegrationTestIngest {
  @Override
  protected void initTable() throws IOException {
    // Do the same as the LoadTestTool does, but with different table configuration.
    HTableDescriptor htd = new HTableDescriptor(getTablename());
    htd.setConfiguration(StoreEngine.STORE_ENGINE_CLASS_KEY, StripeStoreEngine.class.getName());
    htd.setConfiguration(HStore.BLOCKING_STOREFILES_KEY, "100");
    HColumnDescriptor hcd = new HColumnDescriptor(LoadTestTool.COLUMN_FAMILY);
    HBaseTestingUtility.createPreSplitLoadTestTable(util.getConfiguration(), htd, hcd);
  }
}
