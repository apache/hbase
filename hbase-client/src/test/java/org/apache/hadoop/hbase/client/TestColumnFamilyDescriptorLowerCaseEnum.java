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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Testcase for HBASE-21732. Make sure that all enum configurations can accept lower case value.
 */
@Category({ ClientTests.class, SmallTests.class })
public class TestColumnFamilyDescriptorLowerCaseEnum {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestColumnFamilyDescriptorLowerCaseEnum.class);

  private static final Logger LOG =
    LoggerFactory.getLogger(TestColumnFamilyDescriptorLowerCaseEnum.class);

  private Method getSetMethod(Method getMethod, Class<?> enumType) throws NoSuchMethodException {
    String methodName = getMethod.getName().replaceFirst("get", "set");
    return ColumnFamilyDescriptorBuilder.class.getMethod(methodName, enumType);
  }

  private Enum<?> getEnumValue(Class<?> enumType) {
    for (Enum<?> enumConst : enumType.asSubclass(Enum.class).getEnumConstants()) {
      if (!enumConst.name().equalsIgnoreCase("NONE") && !enumConst.name().equals("DEFAULT")) {
        return enumConst;
      }
    }
    throw new NoSuchElementException(enumType.getName());
  }

  private boolean contains(Collection<Enum<?>> enumConsts, String value) {
    return enumConsts.stream().anyMatch(e -> e.name().equals(value));
  }

  @Test
  public void test()
      throws IllegalAccessException, InvocationTargetException, NoSuchMethodException {
    Map<Method, Enum<?>> getMethod2Value = new HashMap<>();
    ColumnFamilyDescriptorBuilder builder =
      ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("test"));
    for (Method method : ColumnFamilyDescriptor.class.getMethods()) {
      if (method.getParameterCount() == 0 && method.getReturnType().isEnum()) {
        LOG.info("Checking " + method);
        Class<?> enumType = method.getReturnType();
        Method setMethod = getSetMethod(method, enumType);
        Enum<?> enumConst = getEnumValue(enumType);
        LOG.info("Using " + setMethod + " to set the value to " + enumConst);
        setMethod.invoke(builder, enumConst);
        getMethod2Value.put(method, enumConst);
      }
    }
    ColumnFamilyDescriptor desc = builder.build();
    ColumnFamilyDescriptorBuilder builder2 =
      ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("test2"));
    desc.getValues().forEach((k, v) -> {
      LOG.info(k.toString() + "=>" + v.toString());
      String str = Bytes.toString(v.get(), v.getOffset(), v.getLength());
      if (contains(getMethod2Value.values(), str)) {
        LOG.info("Set to lower case " + str.toLowerCase());
        builder2.setValue(k, new Bytes(Bytes.toBytes(str.toLowerCase())));
      }
    });

    ColumnFamilyDescriptor desc2 = builder2.build();
    for (Map.Entry<Method, Enum<?>> entry : getMethod2Value.entrySet()) {
      assertEquals(entry.getKey() + " should return " + entry.getValue(), entry.getValue(),
        entry.getKey().invoke(desc2));
    }
  }
}
