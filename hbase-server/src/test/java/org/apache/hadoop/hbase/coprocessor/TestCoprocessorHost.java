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
package org.apache.hadoop.hbase.coprocessor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.lang.reflect.InvocationTargetException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.ClassLoaderTestHelper;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({SmallTests.class})
public class TestCoprocessorHost {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCoprocessorHost.class);

  private static final HBaseCommonTestingUtility TEST_UTIL = new HBaseCommonTestingUtility();

  /**
   * An {@link Abortable} implementation for tests.
   */
  private static class TestAbortable implements Abortable {
    private volatile boolean aborted = false;

    @Override
    public void abort(String why, Throwable e) {
      this.aborted = true;
      Assert.fail(e.getMessage());
    }

    @Override
    public boolean isAborted() {
      return this.aborted;
    }
  }

  @Test
  public void testDoubleLoadingAndPriorityValue() {
    final Configuration conf = HBaseConfiguration.create();
    final String key = "KEY";
    final String coprocessor = "org.apache.hadoop.hbase.coprocessor.SimpleRegionObserver";

    CoprocessorHost<RegionCoprocessor, CoprocessorEnvironment<RegionCoprocessor>> host;
    host = new CoprocessorHostForTest<>(conf);
    int overridePriority = Integer.MAX_VALUE - 1;

    final String coprocessor_v3 =
      SimpleRegionObserverV3.class.getName() + "|" + overridePriority;

    // Try and load a coprocessor three times
    conf.setStrings(key, coprocessor, coprocessor, coprocessor,
        SimpleRegionObserverV2.class.getName(), coprocessor_v3);
    host.loadSystemCoprocessors(conf, key);

    // Three coprocessors(SimpleRegionObserver, SimpleRegionObserverV2,
    // SimpleRegionObserverV3) loaded
    Assert.assertEquals(3, host.coprocEnvironments.size());

    // Check the priority value
    CoprocessorEnvironment<?> simpleEnv = host.findCoprocessorEnvironment(
        SimpleRegionObserver.class.getName());
    CoprocessorEnvironment<?> simpleEnv_v2 = host.findCoprocessorEnvironment(
        SimpleRegionObserverV2.class.getName());
    CoprocessorEnvironment<?> simpleEnv_v3 = host.findCoprocessorEnvironment(
      SimpleRegionObserverV3.class.getName());

    assertNotNull(simpleEnv);
    assertNotNull(simpleEnv_v2);
    assertNotNull(simpleEnv_v3);
    assertEquals(Coprocessor.PRIORITY_SYSTEM, simpleEnv.getPriority());
    assertEquals(Coprocessor.PRIORITY_SYSTEM + 1, simpleEnv_v2.getPriority());
    assertEquals(overridePriority, simpleEnv_v3.getPriority());
  }

  @Test
  public void testLoadSystemCoprocessorWithPath() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    final String key = "KEY";
    final String testClassName = "TestSystemCoprocessor";
    final String testClassNameWithPriorityAndPath = testClassName + "PriorityAndPath";

    File jarFile = buildCoprocessorJar(testClassName);
    File jarFileWithPriorityAndPath = buildCoprocessorJar(testClassNameWithPriorityAndPath);

    try {
      CoprocessorHost<RegionCoprocessor, CoprocessorEnvironment<RegionCoprocessor>> host;
      host = new CoprocessorHostForTest<>(conf);

      // make a string of coprocessor with only priority
      int overridePriority = Integer.MAX_VALUE - 1;
      final String coprocessorWithPriority =
        SimpleRegionObserverV3.class.getName() + "|" + overridePriority;
      // make a string of coprocessor with path but no priority
      final String coprocessorWithPath =
        String.format("%s|%s|%s", testClassName, "", jarFile.getAbsolutePath());
      // make a string of coprocessor with priority and path
      final String coprocessorWithPriorityAndPath = String
        .format("%s|%s|%s", testClassNameWithPriorityAndPath, (overridePriority - 1),
          jarFileWithPriorityAndPath.getAbsolutePath());

      // Try and load a system coprocessors
      conf.setStrings(key, SimpleRegionObserverV2.class.getName(), coprocessorWithPriority,
        coprocessorWithPath, coprocessorWithPriorityAndPath);
      host.loadSystemCoprocessors(conf, key);

      // first loaded system coprocessor with default priority
      CoprocessorEnvironment<?> simpleEnv =
        host.findCoprocessorEnvironment(SimpleRegionObserverV2.class.getName());
      assertNotNull(simpleEnv);
      assertEquals(Coprocessor.PRIORITY_SYSTEM, simpleEnv.getPriority());

      // external system coprocessor with default priority
      CoprocessorEnvironment<?> coprocessorEnvironmentWithPath =
        host.findCoprocessorEnvironment(testClassName);
      assertNotNull(coprocessorEnvironmentWithPath);
      assertEquals(Coprocessor.PRIORITY_SYSTEM + 1, coprocessorEnvironmentWithPath.getPriority());

      // system coprocessor with configured priority
      CoprocessorEnvironment<?> coprocessorEnvironmentWithPriority =
        host.findCoprocessorEnvironment(SimpleRegionObserverV3.class.getName());
      assertNotNull(coprocessorEnvironmentWithPriority);
      assertEquals(overridePriority, coprocessorEnvironmentWithPriority.getPriority());

      // external system coprocessor with override priority
      CoprocessorEnvironment<?> coprocessorEnvironmentWithPriorityAndPath =
        host.findCoprocessorEnvironment(testClassNameWithPriorityAndPath);
      assertNotNull(coprocessorEnvironmentWithPriorityAndPath);
      assertEquals(overridePriority - 1, coprocessorEnvironmentWithPriorityAndPath.getPriority());
    } finally {
      if (jarFile.exists()) {
        jarFile.delete();
      }
      if (jarFileWithPriorityAndPath.exists()) {
        jarFileWithPriorityAndPath.delete();
      }
    }
  }

  @Test(expected = AssertionError.class)
  public void testLoadSystemCoprocessorWithPathDoesNotExist() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    final String key = "KEY";
    final String testClassName = "TestSystemCoprocessor";

    CoprocessorHost<RegionCoprocessor, CoprocessorEnvironment<RegionCoprocessor>> host;
    host = new CoprocessorHostForTest<>(conf);

    // make a string of coprocessor with path but no priority
    final String coprocessorWithPath = testClassName + "||" + testClassName + ".jar";

    // Try and load a system coprocessors
    conf.setStrings(key, coprocessorWithPath);
    // when loading non-exist with CoprocessorHostForTest host, it aborts with AssertionError
    host.loadSystemCoprocessors(conf, key);
  }

  @Test(expected = AssertionError.class)
  public void testLoadSystemCoprocessorWithPathDoesNotExistAndPriority() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    final String key = "KEY";
    final String testClassName = "TestSystemCoprocessor";

    CoprocessorHost<RegionCoprocessor, CoprocessorEnvironment<RegionCoprocessor>> host;
    host = new CoprocessorHostForTest<>(conf);

    int overridePriority = Integer.MAX_VALUE - 1;
    // make a string of coprocessor with path and priority
    final String coprocessor =
      testClassName + "|" + overridePriority + "|" + testClassName + ".jar";

    // Try and load a system coprocessors
    conf.setStrings(key, coprocessor);
    // when loading non-exist coprocessor, it aborts with AssertionError
    host.loadSystemCoprocessors(conf, key);
  }

  public static class SimpleRegionObserverV2 extends SimpleRegionObserver { }

  public static class SimpleRegionObserverV3 extends SimpleRegionObserver {

  }

  private static class CoprocessorHostForTest<E extends Coprocessor> extends
      CoprocessorHost<E, CoprocessorEnvironment<E>> {
    final Configuration cpHostConf;

    public CoprocessorHostForTest(Configuration conf) {
      super(new TestAbortable());
      cpHostConf = conf;
    }

    @Override
    public E checkAndGetInstance(Class<?> implClass)
        throws InstantiationException, IllegalAccessException {
      try {
        return (E) implClass.getDeclaredConstructor().newInstance();
      } catch (InvocationTargetException | NoSuchMethodException e) {
        throw (InstantiationException) new InstantiationException().initCause(e);
      }
    }

    @Override
    public CoprocessorEnvironment<E> createEnvironment(final E instance, final int priority,
        int sequence, Configuration conf) {
      return new BaseEnvironment<>(instance, priority, 0, cpHostConf);
    }
  }

  private File buildCoprocessorJar(String className) throws Exception {
    String dataTestDir = TEST_UTIL.getDataTestDir().toString();
    String code = String.format("import org.apache.hadoop.hbase.coprocessor.*; public class %s"
      + " implements RegionCoprocessor {}", className);
    return ClassLoaderTestHelper.buildJar(dataTestDir, className, code);
  }
}
