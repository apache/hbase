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
package org.apache.hadoop.hbase.client;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
@Category({ LargeTests.class, ClientTests.class })
public class TestRawAsyncTableScan extends AbstractTestAsyncTableScan {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRawAsyncTableScan.class);

  @Parameter(0)
  public String scanType;

  @Parameter(1)
  public Supplier<Scan> scanCreater;

  @Parameters(name = "{index}: type={0}")
  public static List<Object[]> params() {
    return getScanCreatorParams();
  }

  @Override
  protected Scan createScan() {
    return scanCreater.get();
  }

  @Override
  protected List<Result> doScan(Scan scan, int closeAfter) throws Exception {
    BufferingScanResultConsumer scanConsumer = new BufferingScanResultConsumer();
    ASYNC_CONN.getTable(TABLE_NAME).scan(scan, scanConsumer);
    List<Result> results = new ArrayList<>();
    // these tests batch settings with the sample data result in each result being
    // split in two. so we must allow twice the expected results in order to reach
    // our true limit. see convertFromBatchResult for details.
    if (closeAfter > 0 && scan.getBatch() > 0) {
      closeAfter = closeAfter * 2;
    }
    for (Result result; (result = scanConsumer.take()) != null;) {
      results.add(result);
      if (closeAfter > 0 && results.size() >= closeAfter) {
        break;
      }
    }
    if (scan.getBatch() > 0) {
      results = convertFromBatchResult(results);
    }
    return results;
  }
}
