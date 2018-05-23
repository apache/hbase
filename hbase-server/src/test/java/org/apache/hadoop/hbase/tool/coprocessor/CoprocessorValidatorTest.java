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

package org.apache.hadoop.hbase.tool.coprocessor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.tool.coprocessor.CoprocessorViolation.Severity;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

@Category({ SmallTests.class })
@SuppressWarnings("deprecation")
public class CoprocessorValidatorTest {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(CoprocessorValidatorTest.class);

  private CoprocessorValidator validator;

  public CoprocessorValidatorTest() {
    validator = new CoprocessorValidator();
  }

  private static ClassLoader getClassLoader() {
    return CoprocessorValidatorTest.class.getClassLoader();
  }

  private static String getFullClassName(String className) {
    return CoprocessorValidatorTest.class.getName() + "$" + className;
  }

  @SuppressWarnings({"rawtypes", "unused"})
  private static class TestObserver implements Coprocessor {
    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
    }
  }

  @Test
  public void testFilterObservers() throws Exception {
    String filterObservers = getFullClassName("TestObserver");
    List<String> classNames = Lists.newArrayList(
        filterObservers, getClass().getName());
    List<String> filteredClassNames = validator.filterObservers(getClassLoader(), classNames);

    assertEquals(1, filteredClassNames.size());
    assertEquals(filterObservers, filteredClassNames.get(0));
  }

  private List<CoprocessorViolation> validate(String className) {
    ClassLoader classLoader = getClass().getClassLoader();
    return validate(classLoader, className);
  }

  private List<CoprocessorViolation> validate(ClassLoader classLoader, String className) {
    List<String> classNames = Lists.newArrayList(getClass().getName() + "$" + className);
    return validator.validate(classLoader, classNames);
  }

  /*
   * In this test case, we are try to load a not-existent class.
   */
  @Test
  public void testNoSuchClass() throws IOException {
    List<CoprocessorViolation> violations = validate("NoSuchClass");
    assertEquals(1, violations.size());

    CoprocessorViolation violation = violations.get(0);
    assertEquals(Severity.ERROR, violation.getSeverity());
    assertTrue(violation.getMessage().contains(
        "java.lang.ClassNotFoundException: " +
        "org.apache.hadoop.hbase.tool.coprocessor.CoprocessorValidatorTest$NoSuchClass"));
  }

  /*
   * In this test case, we are validating MissingClass coprocessor, which
   * references a missing class. With a special classloader, we prevent that
   * class to be loaded at runtime. It simulates similar cases where a class
   * is no more on our classpath.
   * E.g. org.apache.hadoop.hbase.regionserver.wal.WALEdit was moved to
   * org.apache.hadoop.hbase.wal, so class loading will fail on 2.0.
   */
  private static class MissingClass {
  }

  @SuppressWarnings("unused")
  private static class MissingClassObserver {
    public void method(MissingClass missingClass) {
    }
  }

  private static class MissingClassClassLoader extends ClassLoader {
    public MissingClassClassLoader() {
      super(getClassLoader());
    }

    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException {
      if (name.equals(getFullClassName("MissingClass"))) {
        throw new ClassNotFoundException(name);
      }

      return super.findClass(name);
    }
  }

  @Test
  public void testMissingClass() throws IOException {
    MissingClassClassLoader missingClassClassLoader = new MissingClassClassLoader();
    List<CoprocessorViolation> violations = validate(missingClassClassLoader,
        "MissingClassObserver");
    assertEquals(1, violations.size());

    CoprocessorViolation violation = violations.get(0);
    assertEquals(Severity.ERROR, violation.getSeverity());
    assertTrue(violation.getMessage().contains(
        "java.lang.ClassNotFoundException: " +
        "org.apache.hadoop.hbase.tool.coprocessor.CoprocessorValidatorTest$MissingClass"));
  }

  /*
   * ObsoleteMethod coprocessor implements preCreateTable method which has
   * HRegionInfo parameters. In our current implementation, we pass only
   * RegionInfo parameters, so this method won't be called by HBase at all.
   */
  @SuppressWarnings("unused")
  private static class ObsoleteMethodObserver /* implements MasterObserver */ {
    public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
        HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
    }
  }

  @Test
  public void testObsoleteMethod() throws IOException {
    List<CoprocessorViolation> violations = validate("ObsoleteMethodObserver");
    assertEquals(1, violations.size());

    CoprocessorViolation violation = violations.get(0);
    assertEquals(Severity.WARNING, violation.getSeverity());
    assertTrue(violation.getMessage().contains("was removed from new coprocessor API"));
  }
}
