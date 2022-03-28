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

package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MetricsTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MetricsTests.class, SmallTests.class})
public class TestMetricsUserSourceImpl {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMetricsUserSourceImpl.class);

  @SuppressWarnings("SelfComparison")
  @Test
  public void testCompareToHashCodeEquals() throws Exception {
    MetricsRegionServerSourceFactory fact
      = CompatibilitySingletonFactory.getInstance(MetricsRegionServerSourceFactory.class);

    MetricsUserSource one = fact.createUser("ONE");
    MetricsUserSource oneClone = fact.createUser("ONE");
    MetricsUserSource two = fact.createUser("TWO");

    assertEquals(0, one.compareTo(oneClone));
    assertEquals(one.hashCode(), oneClone.hashCode());
    assertNotEquals(one, two);

    assertTrue(one.compareTo(two) != 0);
    assertTrue(two.compareTo(one) != 0);
    assertTrue(two.compareTo(one) != one.compareTo(two));
    assertTrue(two.compareTo(two) == 0);
  }


  @Test (expected = RuntimeException.class)
  public void testNoGetRegionServerMetricsSourceImpl() throws Exception {
    // This should throw an exception because MetricsUserSourceImpl should only
    // be created by a factory.
    CompatibilitySingletonFactory.getInstance(MetricsUserSource.class);
  }

  @Test
  public void testGetUser() {
    MetricsRegionServerSourceFactory fact
      = CompatibilitySingletonFactory.getInstance(MetricsRegionServerSourceFactory.class);

    MetricsUserSource one = fact.createUser("ONE");
    assertEquals("ONE", one.getUser());
  }

}
