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

import java.lang.reflect.InvocationTargetException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({SmallTests.class})
public class TestCoprocessorHost {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCoprocessorHost.class);

  /**
   * An {@link Abortable} implementation for tests.
   */
  private static class TestAbortable implements Abortable {
    private volatile boolean aborted = false;

    @Override
    public void abort(String why, Throwable e) {
      this.aborted = true;
      Assert.fail();
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

    // Try and load a coprocessor three times
    conf.setStrings(key, coprocessor, coprocessor, coprocessor,
        SimpleRegionObserverV2.class.getName());
    host.loadSystemCoprocessors(conf, key);

    // Two coprocessors(SimpleRegionObserver and SimpleRegionObserverV2) loaded
    Assert.assertEquals(2, host.coprocEnvironments.size());

    // Check the priority value
    CoprocessorEnvironment<?> simpleEnv = host.findCoprocessorEnvironment(
        SimpleRegionObserver.class.getName());
    CoprocessorEnvironment<?> simpleEnv_v2 = host.findCoprocessorEnvironment(
        SimpleRegionObserverV2.class.getName());

    assertNotNull(simpleEnv);
    assertNotNull(simpleEnv_v2);
    assertEquals(Coprocessor.PRIORITY_SYSTEM, simpleEnv.getPriority());
    assertEquals(Coprocessor.PRIORITY_SYSTEM + 1, simpleEnv_v2.getPriority());
  }

  public static class SimpleRegionObserverV2 extends SimpleRegionObserver { }

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
}
