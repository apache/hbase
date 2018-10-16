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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.jar.JarOutputStream;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.CoprocessorDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.tool.coprocessor.CoprocessorViolation.Severity;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.base.Throwables;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.io.ByteStreams;

@Category({ SmallTests.class })
@SuppressWarnings("deprecation")
public class CoprocessorValidatorTest {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(CoprocessorValidatorTest.class);

  private CoprocessorValidator validator;

  public CoprocessorValidatorTest() {
    validator = new CoprocessorValidator();
    validator.setConf(HBaseConfiguration.create());
  }

  private static ClassLoader getClassLoader() {
    return CoprocessorValidatorTest.class.getClassLoader();
  }

  private static String getFullClassName(String className) {
    return CoprocessorValidatorTest.class.getName() + "$" + className;
  }

  private List<CoprocessorViolation> validateClass(String className) {
    ClassLoader classLoader = getClass().getClassLoader();
    return validateClass(classLoader, className);
  }

  private List<CoprocessorViolation> validateClass(ClassLoader classLoader, String className) {
    List<String> classNames = Lists.newArrayList(getFullClassName(className));
    List<CoprocessorViolation> violations = new ArrayList<>();

    validator.validateClasses(classLoader, classNames, violations);

    return violations;
  }

  /*
   * In this test case, we are try to load a not-existent class.
   */
  @Test
  public void testNoSuchClass() throws IOException {
    List<CoprocessorViolation> violations = validateClass("NoSuchClass");
    assertEquals(1, violations.size());

    CoprocessorViolation violation = violations.get(0);
    assertEquals(getFullClassName("NoSuchClass"), violation.getClassName());
    assertEquals(Severity.ERROR, violation.getSeverity());

    String stackTrace = Throwables.getStackTraceAsString(violation.getThrowable());
    assertTrue(stackTrace.contains("java.lang.ClassNotFoundException: " +
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
    List<CoprocessorViolation> violations = validateClass(missingClassClassLoader,
        "MissingClassObserver");
    assertEquals(1, violations.size());

    CoprocessorViolation violation = violations.get(0);
    assertEquals(getFullClassName("MissingClassObserver"), violation.getClassName());
    assertEquals(Severity.ERROR, violation.getSeverity());

    String stackTrace = Throwables.getStackTraceAsString(violation.getThrowable());
    assertTrue(stackTrace.contains("java.lang.ClassNotFoundException: " +
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
    List<CoprocessorViolation> violations = validateClass("ObsoleteMethodObserver");
    assertEquals(1, violations.size());

    CoprocessorViolation violation = violations.get(0);
    assertEquals(Severity.WARNING, violation.getSeverity());
    assertEquals(getFullClassName("ObsoleteMethodObserver"), violation.getClassName());
    assertTrue(violation.getMessage().contains("was removed from new coprocessor API"));
  }

  private List<CoprocessorViolation> validateTable(String jarFile, String className)
      throws IOException {
    Pattern pattern = Pattern.compile(".*");

    Admin admin = mock(Admin.class);

    TableDescriptor tableDescriptor = mock(TableDescriptor.class);
    List<TableDescriptor> tableDescriptors = Lists.newArrayList(tableDescriptor);
    doReturn(tableDescriptors).when(admin).listTableDescriptors(pattern);

    CoprocessorDescriptor coprocessorDescriptor = mock(CoprocessorDescriptor.class);
    List<CoprocessorDescriptor> coprocessorDescriptors =
        Lists.newArrayList(coprocessorDescriptor);
    doReturn(coprocessorDescriptors).when(tableDescriptor).getCoprocessorDescriptors();

    doReturn(getFullClassName(className)).when(coprocessorDescriptor).getClassName();
    doReturn(Optional.ofNullable(jarFile)).when(coprocessorDescriptor).getJarPath();

    List<CoprocessorViolation> violations = new ArrayList<>();

    validator.validateTables(getClassLoader(), admin, pattern, violations);

    return violations;
  }

  @Test
  public void testTableNoSuchClass() throws IOException {
    List<CoprocessorViolation> violations = validateTable(null, "NoSuchClass");
    assertEquals(1, violations.size());

    CoprocessorViolation violation = violations.get(0);
    assertEquals(getFullClassName("NoSuchClass"), violation.getClassName());
    assertEquals(Severity.ERROR, violation.getSeverity());

    String stackTrace = Throwables.getStackTraceAsString(violation.getThrowable());
    assertTrue(stackTrace.contains("java.lang.ClassNotFoundException: " +
        "org.apache.hadoop.hbase.tool.coprocessor.CoprocessorValidatorTest$NoSuchClass"));
  }

  @Test
  public void testTableMissingJar() throws IOException {
    List<CoprocessorViolation> violations = validateTable("no such file", "NoSuchClass");
    assertEquals(1, violations.size());

    CoprocessorViolation violation = violations.get(0);
    assertEquals(getFullClassName("NoSuchClass"), violation.getClassName());
    assertEquals(Severity.ERROR, violation.getSeverity());
    assertTrue(violation.getMessage().contains("could not validate jar file 'no such file'"));
  }

  @Test
  public void testTableValidJar() throws IOException {
    Path outputDirectory = Paths.get("target", "test-classes");
    String className = getFullClassName("ObsoleteMethodObserver");
    Path classFile = Paths.get(className.replace('.', '/') + ".class");
    Path fullClassFile = outputDirectory.resolve(classFile);

    Path tempJarFile = Files.createTempFile("coprocessor-validator-test-", ".jar");

    try {
      try (OutputStream fileStream = Files.newOutputStream(tempJarFile);
          JarOutputStream jarStream = new JarOutputStream(fileStream);
          InputStream classStream = Files.newInputStream(fullClassFile)) {
        ZipEntry entry = new ZipEntry(classFile.toString());
        jarStream.putNextEntry(entry);

        ByteStreams.copy(classStream, jarStream);
      }

      String tempJarFileUri = tempJarFile.toUri().toString();

      List<CoprocessorViolation> violations =
          validateTable(tempJarFileUri, "ObsoleteMethodObserver");
      assertEquals(1, violations.size());

      CoprocessorViolation violation = violations.get(0);
      assertEquals(getFullClassName("ObsoleteMethodObserver"), violation.getClassName());
      assertEquals(Severity.WARNING, violation.getSeverity());
      assertTrue(violation.getMessage().contains("was removed from new coprocessor API"));
    } finally {
      Files.delete(tempJarFile);
    }
  }
}
