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

import static junit.framework.TestCase.assertEquals;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.apache.hbase.thirdparty.com.google.common.collect.Iterables;

/**
 * Tests HBaseClassTestRule.
 */
@Category(SmallTests.class)
public class TestHBaseClassTestRule {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule.forClass(
      TestHBaseClassTestRule.class);

  // Test input classes of various kinds.
  private static class NonParameterizedClass {
    void dummy() {
    }
    int dummy(int a) {
      return 0;
    }
  }

  @RunWith(Parameterized.class)
  private static class ParameterizedClassWithNoParametersMethod {
    void dummy() {
    }
  }

  @RunWith(Parameterized.class)
  private static class InValidParameterizedClass {
    // Not valid because parameters method is private.
    @Parameters
    private static List<Object> parameters() {
      return Arrays.asList(1, 2, 3, 4);
    }
    int dummy(int a) {
      return 0;
    }
  }

  @RunWith(Parameterized.class)
  private static class ValidParameterizedClass1 {
    @Parameters
    public static List<Object> parameters() {
      return Arrays.asList(1, 2, 3, 4, 5);
    }
    int dummy(int a) {
      return 0;
    }
  }

  @RunWith(Parameterized.class)
  private static class ValidParameterizedClass2 {
    @Parameters
    public static Object[] parameters() {
      return new Integer[] {1, 2, 3, 4, 5, 6};
    }
  }

  @RunWith(Parameterized.class)
  private static class ValidParameterizedClass3 {
    @Parameters
    public static Iterable<Integer> parameters() {
      return Arrays.asList(1, 2, 3, 4, 5, 6, 7);
    }
  }

  @RunWith(Parameterized.class)
  private static class ValidParameterizedClass4 {
    @Parameters
    public static Collection<Integer> parameters() {
      return Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
    }
  }


  @RunWith(Parameterized.class)
  private static class ExtendedParameterizedClass1 extends ValidParameterizedClass1 {
    // Should be inferred from the parent class.
    int dummy2(int a) {
      return 0;
    }
  }

  @RunWith(Parameterized.class)
  private static class ExtendedParameterizedClass2 extends ValidParameterizedClass1 {
    // Should override the parent parameters class.
    @Parameters
    public static List<Object> parameters() {
      return Arrays.asList(1, 2, 3);
    }
  }

  @Test
  public void testNumParameters() {
    // Invalid cases, expected to return 1.
    assertEquals(HBaseClassTestRule.getNumParameters(NonParameterizedClass.class), 1);
    assertEquals(HBaseClassTestRule.getNumParameters(
        ParameterizedClassWithNoParametersMethod.class), 1);
    assertEquals(HBaseClassTestRule.getNumParameters(InValidParameterizedClass.class), 1);
    // Valid parameterized classes.
    assertEquals(HBaseClassTestRule.getNumParameters(ValidParameterizedClass1.class),
        ValidParameterizedClass1.parameters().size());
    assertEquals(HBaseClassTestRule.getNumParameters(ValidParameterizedClass2.class),
        ValidParameterizedClass2.parameters().length);
    assertEquals(HBaseClassTestRule.getNumParameters(ValidParameterizedClass3.class),
        Iterables.size(ValidParameterizedClass3.parameters()));
    assertEquals(HBaseClassTestRule.getNumParameters(ValidParameterizedClass4.class),
        ValidParameterizedClass4.parameters().size());
    // Testing inheritance.
    assertEquals(HBaseClassTestRule.getNumParameters(ExtendedParameterizedClass1.class),
        ValidParameterizedClass1.parameters().size());
    assertEquals(HBaseClassTestRule.getNumParameters(ExtendedParameterizedClass2.class),
        ExtendedParameterizedClass2.parameters().size());
  }
}
