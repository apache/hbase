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

package org.apache.hadoop.hbase.thrift.metrics;

import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 *    Test the hadoop 1 version of ThriftServerMetricsSourceFactory
 */
public class TestThriftServerMetricsSourceFactoryImpl {

  @Test
  public void testCompatabilityRegistered() throws Exception {
    assertNotNull(CompatibilitySingletonFactory.getInstance(ThriftServerMetricsSourceFactory.class));
    assertTrue(CompatibilitySingletonFactory.getInstance(ThriftServerMetricsSourceFactory.class) instanceof ThriftServerMetricsSourceFactoryImpl);
  }

  @Test
  public void testCreateThriftOneSource() throws Exception {
    //Make sure that the factory gives back a singleton.
    assertSame(new ThriftServerMetricsSourceFactoryImpl().createThriftOneSource(),
        new ThriftServerMetricsSourceFactoryImpl().createThriftOneSource());

  }

  @Test
  public void testCreateThriftTwoSource() throws Exception {
    //Make sure that the factory gives back a singleton.
    assertSame(new ThriftServerMetricsSourceFactoryImpl().createThriftTwoSource(),
        new ThriftServerMetricsSourceFactoryImpl().createThriftTwoSource());
  }
}
