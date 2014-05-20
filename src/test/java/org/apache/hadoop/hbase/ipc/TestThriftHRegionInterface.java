/**
 * Copyright The Apache Software Foundation
 *
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

package org.apache.hadoop.hbase.ipc;

import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.service.ThriftMethod;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.hbase.Restartable;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.StopStatus;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.coprocessor.endpoints.IEndpointService;
import org.apache.hadoop.hbase.regionserver.HRegionServerIf;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * Test to make sure that our thrift interface is well defined and that Async and Sync interfaces are
 * kept in sync.
 */
@Category(SmallTests.class)
public class TestThriftHRegionInterface {

  @Test
  public void testMethodAnnotations() {
    Map<String, Method> nameToSyncMethods = new HashMap<>(55);
    Map<String, Method> nameToAsyncMethods = new HashMap<>(55);

    for (Method m : ThriftHRegionInterface.Sync.class.getDeclaredMethods()) {
      String thriftName = getThriftMethodName(m);
      assertNotNull(thriftName);
      assertNull("Each thrift named method should be unique. [ " + thriftName + " ]",
          nameToSyncMethods.get(thriftName));
      nameToSyncMethods.put(thriftName, m);
      assertAnnotatedWell(m);
    }

    for (Method asyncMethod : ThriftHRegionInterface.Async.class.getDeclaredMethods()) {
      String thriftName = getThriftMethodName(asyncMethod);
      assertNotNull(thriftName);
      Method syncMethod = nameToSyncMethods.get(thriftName);
      assertNotNull("expecting " + thriftName + " to be in nameToSyncMethods", syncMethod);
      assertNull("Each thrift named method should be unique. [ " + thriftName + " ]",
          nameToAsyncMethods.get(thriftName));
      nameToAsyncMethods.put(thriftName, asyncMethod);
      assertAnnotatedWell(asyncMethod);
      assertEquals(asyncMethod.getAnnotation(ThriftMethod.class),
          syncMethod.getAnnotation(ThriftMethod.class));

      Class<?> klass = asyncMethod.getReturnType();
      assertEquals(
          "Every method in the asyncMethod should return a future [ " + asyncMethod.getName()
              + " ] = " + klass, ListenableFuture.class.getName(),
          klass.getName());
    }

    assertEquals(ThriftHRegionInterface.Sync.class.getDeclaredMethods().length,
        nameToSyncMethods.size());
    assertEquals(ThriftHRegionInterface.Async.class.getDeclaredMethods().length,
        nameToAsyncMethods.size());
  }

  @Test
  public void testImplementsHRegionInterface() throws Exception {
    assertEquals(HRegionInterface.class.getDeclaredMethods().length
            + Restartable.class.getDeclaredMethods().length
            + StopStatus.class.getDeclaredMethods().length
            + IEndpointService.class.getDeclaredMethods().length
            + IRegionScanService.class.getDeclaredMethods().length
            - 1 /* getOnlineRegionsAsArray is test only*/,
        ThriftHRegionInterface.Async.class.getDeclaredMethods().length);

    assertEquals(HRegionInterface.class.getDeclaredMethods().length
            + Restartable.class.getDeclaredMethods().length
            + StopStatus.class.getDeclaredMethods().length
            + IEndpointService.class.getDeclaredMethods().length
            + IRegionScanService.class.getDeclaredMethods().length
            - 1 /* getOnlineRegionsAsArray is test only*/,
        ThriftHRegionInterface.Sync.class.getDeclaredMethods().length);

    assertEquals(ThriftHRegionInterface.Sync.class.getDeclaredMethods().length,
        ThriftHRegionInterface.Async.class.getDeclaredMethods().length);
  }

  protected void assertAnnotatedWell(Method m) {
    ThriftMethod tm = m.getAnnotation(ThriftMethod.class);
    assertNotNull("every method should have a ThriftMethod annotation", tm);
    assertNotNull("There should be an exception annotation", tm.exception());
    assertTrue("There should be an exception annotation", tm.exception().length > 0);

    for (Annotation[] parameterAnnotations:m.getParameterAnnotations()) {

      ThriftField thriftField = null;

      for (Annotation a:parameterAnnotations) {
        if (a instanceof ThriftField) {
          thriftField = (ThriftField) a;
        }
      }

      assertNotNull(thriftField);
      assertNotNull(thriftField.name());
      assertTrue("Every parameter should have a thriftField name", thriftField.name().length() > 1);
    }
  }

  protected String getThriftMethodName(Method m) {
    String thriftName = null;
    Annotation[] annotations = m.getAnnotations();
    for (Annotation a : annotations) {
      if (a instanceof ThriftMethod) {
        thriftName = ((ThriftMethod) a).value();
      }
    }
    return thriftName;
  }
}
