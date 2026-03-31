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
package org.apache.hadoop.hbase;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.reflect.Field;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Verify that the values are all correct.
 */
@Tag(MiscTests.TAG)
@Tag(SmallTests.TAG)
public class TestJUnit5TagConstants {

  @Test
  public void testVerify() throws Exception {
    ClassFinder finder = new ClassFinder(getClass().getClassLoader());
    for (Class<?> annoClazz : finder.findClasses(ClientTests.class.getPackage().getName(), false)) {
      Field field = annoClazz.getField("TAG");
      assertEquals(annoClazz.getName(), field.get(null));
    }
  }
}
