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

import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.testclassification.MetricsTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 *  Test for MetricsRegionServerSourceImpl
 */
@Category({MetricsTests.class, SmallTests.class})
public class TestMetricsRegionServerSourceImpl {

  @Test
  public void testGetInstance() throws Exception {
    MetricsRegionServerSourceFactory metricsRegionServerSourceFactory =
        CompatibilitySingletonFactory.getInstance(MetricsRegionServerSourceFactory.class);
    MetricsRegionServerSource serverSource =
        metricsRegionServerSourceFactory.createServer(null);
    assertTrue(serverSource instanceof MetricsRegionServerSourceImpl);
    assertSame(metricsRegionServerSourceFactory,
        CompatibilitySingletonFactory.getInstance(MetricsRegionServerSourceFactory.class));
  }


  @Test(expected = RuntimeException.class)
  public void testNoGetRegionServerMetricsSourceImpl() throws Exception {
    // This should throw an exception because MetricsRegionServerSourceImpl should only
    // be created by a factory.
    CompatibilitySingletonFactory.getInstance(MetricsRegionServerSourceImpl.class);
  }
}
