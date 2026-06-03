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
package org.junit;

import com.google.errorprone.annotations.RestrictedApi;
import org.junit.jupiter.api.Assertions;

/**
 * JUnit4-compatible shim that delegates all calls to JUnit5 {@link Assertions}. Exists so that
 * third-party test utilities(especially hadoop mini cluster related classes) compiled against
 * JUnit4 can resolve {@code org.junit.Assert} without pulling in the real junit4 dependency.
 */
public final class Assert {

  private Assert() {
  }

  @RestrictedApi(explanation = "Only for thirdparty code, use JUnit5 Assertions in HBase")
  public static void assertTrue(String message, boolean condition) {
    Assertions.assertTrue(condition, message);
  }

  @RestrictedApi(explanation = "Only for thirdparty code, use JUnit5 Assertions in HBase")
  public static void assertTrue(boolean condition) {
    Assertions.assertTrue(condition);
  }

  @RestrictedApi(explanation = "Only for thirdparty code, use JUnit5 Assertions in HBase")
  public static void assertFalse(String message, boolean condition) {
    Assertions.assertFalse(condition, message);
  }

  @RestrictedApi(explanation = "Only for thirdparty code, use JUnit5 Assertions in HBase")
  public static void assertFalse(boolean condition) {
    Assertions.assertFalse(condition);
  }

  @RestrictedApi(explanation = "Only for thirdparty code, use JUnit5 Assertions in HBase")
  public static void fail(String message) {
    Assertions.fail(message);
  }

  @RestrictedApi(explanation = "Only for thirdparty code, use JUnit5 Assertions in HBase")
  public static void fail() {
    Assertions.fail();
  }

  @RestrictedApi(explanation = "Only for thirdparty code, use JUnit5 Assertions in HBase")
  public static void assertEquals(String message, Object expected, Object actual) {
    Assertions.assertEquals(expected, actual, message);
  }

  @RestrictedApi(explanation = "Only for thirdparty code, use JUnit5 Assertions in HBase")
  public static void assertEquals(Object expected, Object actual) {
    Assertions.assertEquals(expected, actual);
  }

  @RestrictedApi(explanation = "Only for thirdparty code, use JUnit5 Assertions in HBase")
  public static void assertEquals(String message, long expected, long actual) {
    Assertions.assertEquals(expected, actual, message);
  }

  @RestrictedApi(explanation = "Only for thirdparty code, use JUnit5 Assertions in HBase")
  public static void assertEquals(long expected, long actual) {
    Assertions.assertEquals(expected, actual);
  }

  @RestrictedApi(explanation = "Only for thirdparty code, use JUnit5 Assertions in HBase")
  public static void assertEquals(double expected, double actual, double delta) {
    Assertions.assertEquals(expected, actual, delta);
  }

  @RestrictedApi(explanation = "Only for thirdparty code, use JUnit5 Assertions in HBase")
  public static void assertEquals(String message, double expected, double actual, double delta) {
    Assertions.assertEquals(expected, actual, delta, message);
  }

  @RestrictedApi(explanation = "Only for thirdparty code, use JUnit5 Assertions in HBase")
  public static void assertEquals(float expected, float actual, float delta) {
    Assertions.assertEquals(expected, actual, delta);
  }

  @RestrictedApi(explanation = "Only for thirdparty code, use JUnit5 Assertions in HBase")
  public static void assertEquals(String message, float expected, float actual, float delta) {
    Assertions.assertEquals(expected, actual, delta, message);
  }

  @RestrictedApi(explanation = "Only for thirdparty code, use JUnit5 Assertions in HBase")
  public static void assertNotEquals(String message, Object unexpected, Object actual) {
    Assertions.assertNotEquals(unexpected, actual, message);
  }

  @RestrictedApi(explanation = "Only for thirdparty code, use JUnit5 Assertions in HBase")
  public static void assertNotEquals(Object unexpected, Object actual) {
    Assertions.assertNotEquals(unexpected, actual);
  }

  @RestrictedApi(explanation = "Only for thirdparty code, use JUnit5 Assertions in HBase")
  public static void assertNotEquals(String message, long unexpected, long actual) {
    Assertions.assertNotEquals(unexpected, actual, message);
  }

  @RestrictedApi(explanation = "Only for thirdparty code, use JUnit5 Assertions in HBase")
  public static void assertNotEquals(long unexpected, long actual) {
    Assertions.assertNotEquals(unexpected, actual);
  }

  @RestrictedApi(explanation = "Only for thirdparty code, use JUnit5 Assertions in HBase")
  public static void assertNotEquals(double unexpected, double actual, double delta) {
    Assertions.assertNotEquals(unexpected, actual, delta);
  }

  @RestrictedApi(explanation = "Only for thirdparty code, use JUnit5 Assertions in HBase")
  public static void assertNotEquals(String message, double unexpected, double actual,
    double delta) {
    Assertions.assertNotEquals(unexpected, actual, delta, message);
  }

  @RestrictedApi(explanation = "Only for thirdparty code, use JUnit5 Assertions in HBase")
  public static void assertNotNull(String message, Object object) {
    Assertions.assertNotNull(object, message);
  }

  @RestrictedApi(explanation = "Only for thirdparty code, use JUnit5 Assertions in HBase")
  public static void assertNotNull(Object object) {
    Assertions.assertNotNull(object);
  }

  @RestrictedApi(explanation = "Only for thirdparty code, use JUnit5 Assertions in HBase")
  public static void assertNull(String message, Object object) {
    Assertions.assertNull(object, message);
  }

  @RestrictedApi(explanation = "Only for thirdparty code, use JUnit5 Assertions in HBase")
  public static void assertNull(Object object) {
    Assertions.assertNull(object);
  }

  @RestrictedApi(explanation = "Only for thirdparty code, use JUnit5 Assertions in HBase")
  public static void assertSame(String message, Object expected, Object actual) {
    Assertions.assertSame(expected, actual, message);
  }

  @RestrictedApi(explanation = "Only for thirdparty code, use JUnit5 Assertions in HBase")
  public static void assertSame(Object expected, Object actual) {
    Assertions.assertSame(expected, actual);
  }

  @RestrictedApi(explanation = "Only for thirdparty code, use JUnit5 Assertions in HBase")
  public static void assertNotSame(String message, Object unexpected, Object actual) {
    Assertions.assertNotSame(unexpected, actual, message);
  }

  @RestrictedApi(explanation = "Only for thirdparty code, use JUnit5 Assertions in HBase")
  public static void assertNotSame(Object unexpected, Object actual) {
    Assertions.assertNotSame(unexpected, actual);
  }

  @RestrictedApi(explanation = "Only for thirdparty code, use JUnit5 Assertions in HBase")
  public static void assertArrayEquals(String message, Object[] expecteds, Object[] actuals) {
    Assertions.assertArrayEquals(expecteds, actuals, message);
  }

  @RestrictedApi(explanation = "Only for thirdparty code, use JUnit5 Assertions in HBase")
  public static void assertArrayEquals(Object[] expecteds, Object[] actuals) {
    Assertions.assertArrayEquals(expecteds, actuals);
  }

  @RestrictedApi(explanation = "Only for thirdparty code, use JUnit5 Assertions in HBase")
  public static void assertArrayEquals(String message, byte[] expecteds, byte[] actuals) {
    Assertions.assertArrayEquals(expecteds, actuals, message);
  }

  @RestrictedApi(explanation = "Only for thirdparty code, use JUnit5 Assertions in HBase")
  public static void assertArrayEquals(byte[] expecteds, byte[] actuals) {
    Assertions.assertArrayEquals(expecteds, actuals);
  }

  @RestrictedApi(explanation = "Only for thirdparty code, use JUnit5 Assertions in HBase")
  public static void assertArrayEquals(String message, char[] expecteds, char[] actuals) {
    Assertions.assertArrayEquals(expecteds, actuals, message);
  }

  @RestrictedApi(explanation = "Only for thirdparty code, use JUnit5 Assertions in HBase")
  public static void assertArrayEquals(char[] expecteds, char[] actuals) {
    Assertions.assertArrayEquals(expecteds, actuals);
  }

  @RestrictedApi(explanation = "Only for thirdparty code, use JUnit5 Assertions in HBase")
  public static void assertArrayEquals(String message, short[] expecteds, short[] actuals) {
    Assertions.assertArrayEquals(expecteds, actuals, message);
  }

  @RestrictedApi(explanation = "Only for thirdparty code, use JUnit5 Assertions in HBase")
  public static void assertArrayEquals(short[] expecteds, short[] actuals) {
    Assertions.assertArrayEquals(expecteds, actuals);
  }

  @RestrictedApi(explanation = "Only for thirdparty code, use JUnit5 Assertions in HBase")
  public static void assertArrayEquals(String message, int[] expecteds, int[] actuals) {
    Assertions.assertArrayEquals(expecteds, actuals, message);
  }

  @RestrictedApi(explanation = "Only for thirdparty code, use JUnit5 Assertions in HBase")
  public static void assertArrayEquals(int[] expecteds, int[] actuals) {
    Assertions.assertArrayEquals(expecteds, actuals);
  }

  @RestrictedApi(explanation = "Only for thirdparty code, use JUnit5 Assertions in HBase")
  public static void assertArrayEquals(String message, long[] expecteds, long[] actuals) {
    Assertions.assertArrayEquals(expecteds, actuals, message);
  }

  @RestrictedApi(explanation = "Only for thirdparty code, use JUnit5 Assertions in HBase")
  public static void assertArrayEquals(long[] expecteds, long[] actuals) {
    Assertions.assertArrayEquals(expecteds, actuals);
  }

  @RestrictedApi(explanation = "Only for thirdparty code, use JUnit5 Assertions in HBase")
  public static void assertArrayEquals(String message, double[] expecteds, double[] actuals,
    double delta) {
    Assertions.assertArrayEquals(expecteds, actuals, delta, message);
  }

  @RestrictedApi(explanation = "Only for thirdparty code, use JUnit5 Assertions in HBase")
  public static void assertArrayEquals(double[] expecteds, double[] actuals, double delta) {
    Assertions.assertArrayEquals(expecteds, actuals, delta);
  }

  @RestrictedApi(explanation = "Only for thirdparty code, use JUnit5 Assertions in HBase")
  public static void assertArrayEquals(String message, float[] expecteds, float[] actuals,
    float delta) {
    Assertions.assertArrayEquals(expecteds, actuals, delta, message);
  }

  @RestrictedApi(explanation = "Only for thirdparty code, use JUnit5 Assertions in HBase")
  public static void assertArrayEquals(float[] expecteds, float[] actuals, float delta) {
    Assertions.assertArrayEquals(expecteds, actuals, delta);
  }
}
