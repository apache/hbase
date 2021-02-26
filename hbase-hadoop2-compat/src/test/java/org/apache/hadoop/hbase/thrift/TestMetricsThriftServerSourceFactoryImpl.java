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
package org.apache.hadoop.hbase.thrift;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MetricsTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test for hadoop 2's version of MetricsThriftServerSourceFactory.
 */
@Category({MetricsTests.class, SmallTests.class})
public class TestMetricsThriftServerSourceFactoryImpl {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMetricsThriftServerSourceFactoryImpl.class);

  @Test
  public void testCompatabilityRegistered() {
    assertNotNull(CompatibilitySingletonFactory.getInstance(
            MetricsThriftServerSourceFactory.class));
    assertTrue(CompatibilitySingletonFactory.getInstance(MetricsThriftServerSourceFactory.class)
            instanceof MetricsThriftServerSourceFactoryImpl);
  }

  @Test
  public void testCreateThriftOneSource() {
    //Make sure that the factory gives back a singleton.
    assertSame(new MetricsThriftServerSourceFactoryImpl().createThriftOneSource(),
        new MetricsThriftServerSourceFactoryImpl().createThriftOneSource());

  }

  @Test
  public void testCreateThriftTwoSource() {
    //Make sure that the factory gives back a singleton.
    assertSame(new MetricsThriftServerSourceFactoryImpl().createThriftTwoSource(),
        new MetricsThriftServerSourceFactoryImpl().createThriftTwoSource());
  }
}
