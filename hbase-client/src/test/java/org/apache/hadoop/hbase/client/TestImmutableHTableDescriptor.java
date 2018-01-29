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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.BuilderStyleTest;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({ClientTests.class, SmallTests.class})
public class TestImmutableHTableDescriptor {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestImmutableHTableDescriptor.class);

  @Rule
  public TestName name = new TestName();
  private static final List<Consumer<ImmutableHTableDescriptor>> TEST_FUNCTION = Arrays.asList(
    htd -> htd.setValue("a", "a"),
    htd -> htd.setValue(Bytes.toBytes("a"), Bytes.toBytes("a")),
    htd -> htd.setValue(new Bytes(Bytes.toBytes("a")), new Bytes(Bytes.toBytes("a"))),
    htd -> htd.setCompactionEnabled(false),
    htd -> htd.setConfiguration("aaa", "ccc"),
    htd -> htd.setDurability(Durability.USE_DEFAULT),
    htd -> htd.setFlushPolicyClassName("class"),
    htd -> htd.setMaxFileSize(123),
    htd -> htd.setMemStoreFlushSize(123123123),
    htd -> htd.setNormalizationEnabled(false),
    htd -> htd.setPriority(123),
    htd -> htd.setReadOnly(true),
    htd -> htd.setRegionMemstoreReplication(true),
    htd -> htd.setRegionReplication(123),
    htd -> htd.setRegionSplitPolicyClassName("class"),
    htd -> htd.addFamily(new HColumnDescriptor(Bytes.toBytes("fm"))),
    htd -> htd.remove(new Bytes(Bytes.toBytes("aaa"))),
    htd -> htd.remove("aaa"),
    htd -> htd.remove(Bytes.toBytes("aaa")),
    htd -> htd.removeConfiguration("xxx"),
    htd -> htd.removeFamily(Bytes.toBytes("fm")),
    htd -> {
      try {
        htd.addCoprocessor("xxx");
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  );

  @Test
  public void testImmutable() {
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(name.getMethodName()));
    ImmutableHTableDescriptor immutableHtd = new ImmutableHTableDescriptor(htd);
    TEST_FUNCTION.forEach(f -> {
      try {
        f.accept(immutableHtd);
        fail("ImmutableHTableDescriptor can't be modified!!!");
      } catch (UnsupportedOperationException e) {
      }
    });
  }

  @Test
  public void testImmutableHColumnDescriptor() {
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(name.getMethodName()));
    htd.addFamily(new HColumnDescriptor(Bytes.toBytes("family")));
    ImmutableHTableDescriptor immutableHtd = new ImmutableHTableDescriptor(htd);
    for (HColumnDescriptor hcd : immutableHtd.getColumnFamilies()) {
      assertReadOnly(hcd);
    }
    for (HColumnDescriptor hcd : immutableHtd.getFamilies()) {
      assertReadOnly(hcd);
    }
  }

  private void assertReadOnly(HColumnDescriptor hcd) {
    try {
      hcd.setBlocksize(10);
      fail("ImmutableHColumnDescriptor can't be modified!!!");
    } catch (UnsupportedOperationException e) {
    }
  }

  @Test
  public void testClassMethodsAreBuilderStyle() {
  /* ImmutableHTableDescriptor should have a builder style setup where setXXX/addXXX methods
   * can be chainable together:
   * . For example:
   * ImmutableHTableDescriptor d
   *   = new ImmutableHTableDescriptor()
   *     .setFoo(foo)
   *     .setBar(bar)
   *     .setBuz(buz)
   *
   * This test ensures that all methods starting with "set" returns the declaring object
   */

      BuilderStyleTest.assertClassesAreBuilderStyle(ImmutableHTableDescriptor.class);
  }
}
