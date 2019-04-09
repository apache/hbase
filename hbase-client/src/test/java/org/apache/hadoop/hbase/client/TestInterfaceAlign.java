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

import static org.junit.Assert.assertTrue;

import java.io.Closeable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ ClientTests.class, SmallTests.class })
public class TestInterfaceAlign {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestInterfaceAlign.class);

  /**
   * Test methods name match up
   */
  @Test
  public void testAdminWithAsyncAdmin() {
    List<String> adminMethodNames = getMethodNames(Admin.class);
    List<String> asyncAdminMethodNames = getMethodNames(AsyncAdmin.class);

    // Remove some special methods
    adminMethodNames.remove("getOperationTimeout");
    adminMethodNames.remove("getSyncWaitTimeout");
    adminMethodNames.remove("getConnection");
    adminMethodNames.remove("getConfiguration");
    adminMethodNames.removeAll(getMethodNames(Abortable.class));
    adminMethodNames.removeAll(getMethodNames(Closeable.class));

    adminMethodNames.forEach(method -> {
      boolean contains = asyncAdminMethodNames.contains(method);
      if (method.endsWith("Async")) {
        contains = asyncAdminMethodNames.contains(method.replace("Async", ""));
      }
      assertTrue("Admin method " + method + " should in AsyncAdmin too", contains);
    });
    asyncAdminMethodNames.forEach(method -> {
      boolean contains = adminMethodNames.contains(method);
      if (!contains) {
        contains = adminMethodNames.contains(method + "Async");
      }
      assertTrue("AsyncAdmin method " + method + " should in Admin too", contains);
    });
  }

  private <T> List<String> getMethodNames(Class<T> c) {
    // DON'T use the getDeclaredMethods as we want to check the Public APIs only.
    return Arrays.asList(c.getMethods()).stream().filter(m -> !isDeprecated(m))
      .filter(m -> !Modifier.isStatic(m.getModifiers())).map(Method::getName).distinct()
      .collect(Collectors.toList());
  }

  private boolean isDeprecated(Method method) {
    Annotation[] annotations = method.getDeclaredAnnotations();
    for (Annotation annotation : annotations) {
      if (annotation instanceof Deprecated) {
        return true;
      }
    }
    return false;
  }
}
