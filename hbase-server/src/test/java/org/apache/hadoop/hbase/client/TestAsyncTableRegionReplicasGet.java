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

import static org.junit.Assert.assertArrayEquals;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
@Category({ MediumTests.class, ClientTests.class })
public class TestAsyncTableRegionReplicasGet extends AbstractTestAsyncTableRegionReplicasRead {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAsyncTableRegionReplicasGet.class);

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    startClusterAndCreateTable();
    AsyncTable<?> table = ASYNC_CONN.getTable(TABLE_NAME);
    table.put(new Put(ROW).addColumn(FAMILY, QUALIFIER, VALUE)).get();
    waitUntilAllReplicasHaveRow(ROW);
  }

  @Override
  protected void readAndCheck(AsyncTable<?> table, int replicaId) throws Exception {
    Get get = new Get(ROW).setConsistency(Consistency.TIMELINE);
    if (replicaId >= 0) {
      get.setReplicaId(replicaId);
    }
    assertArrayEquals(VALUE, table.get(get).get().getValue(FAMILY, QUALIFIER));
  }
}
