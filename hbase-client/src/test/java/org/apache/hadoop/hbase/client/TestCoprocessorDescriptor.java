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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ MiscTests.class, SmallTests.class })
public class TestCoprocessorDescriptor {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestCoprocessorDescriptor.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestCoprocessorDescriptor.class);

  @Rule
  public TestName name = new TestName();

  @Test
  public void testBuild() {
    String className = "className";
    String path = "path";
    int priority = 100;
    String propertyKey = "propertyKey";
    String propertyValue = "propertyValue";
    CoprocessorDescriptor cp =
      CoprocessorDescriptorBuilder.newBuilder(className).setJarPath(path).setPriority(priority)
        .setProperty(propertyKey, propertyValue).build();
    assertEquals(className, cp.getClassName());
    assertEquals(path, cp.getJarPath().get());
    assertEquals(priority, cp.getPriority());
    assertEquals(1, cp.getProperties().size());
    assertEquals(propertyValue, cp.getProperties().get(propertyKey));
  }

  @Test
  public void testSetCoprocessor() throws IOException {
    String propertyKey = "propertyKey";
    List<CoprocessorDescriptor> cps = new ArrayList<>();
    for (String className : Arrays.asList("className0", "className1", "className2")) {
      String path = "path";
      int priority = Math.abs(className.hashCode());
      String propertyValue = "propertyValue";
      cps.add(
        CoprocessorDescriptorBuilder.newBuilder(className).setJarPath(path).setPriority(priority)
          .setProperty(propertyKey, propertyValue).build());
    }
    TableDescriptor tableDescriptor =
      TableDescriptorBuilder.newBuilder(TableName.valueOf(name.getMethodName()))
        .setCoprocessors(cps).build();
    for (CoprocessorDescriptor cp : cps) {
      boolean match = false;
      for (CoprocessorDescriptor that : tableDescriptor.getCoprocessorDescriptors()) {
        if (cp.getClassName().equals(that.getClassName())) {
          assertEquals(cp.getJarPath().get(), that.getJarPath().get());
          assertEquals(cp.getPriority(), that.getPriority());
          assertEquals(cp.getProperties().size(), that.getProperties().size());
          assertEquals(cp.getProperties().get(propertyKey), that.getProperties().get(propertyKey));
          match = true;
          break;
        }
      }
      if (!match) {
        fail("expect:" + cp + ", actual:" + tableDescriptor.getCoprocessorDescriptors());
      }
    }
  }
}
