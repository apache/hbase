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

import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * Test ImmutableHRegionInfo
 */
@Category({ClientTests.class, SmallTests.class})
public class TestImmutableHRegionInfo {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestImmutableHRegionInfo.class);

  @Rule
  public TestName name = new TestName();

  private final List<Consumer<ImmutableHRegionInfo>> TEST_FUNCTIONS = Arrays.asList(
    hri -> hri.setOffline(true),
    hri -> hri.setSplit(true)
  );

  @Test
  public void testImmutable() {
    HRegionInfo hri = new HRegionInfo(TableName.valueOf(name.getMethodName()));
    ImmutableHRegionInfo immutableHri = new ImmutableHRegionInfo(hri);

    TEST_FUNCTIONS.forEach(f -> {
      try {
        f.accept(immutableHri);
        fail("ImmutableHRegionInfo can't be modified !!!");
      } catch(UnsupportedOperationException e) {
      }
    });
  }

}
