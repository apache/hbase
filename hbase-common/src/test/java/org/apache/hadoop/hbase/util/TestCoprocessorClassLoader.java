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
package org.apache.hadoop.hbase.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.io.IOUtils;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test TestCoprocessorClassLoader. More tests are in TestClassLoading
 */
@Category({MiscTests.class, SmallTests.class})
public class TestCoprocessorClassLoader {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCoprocessorClassLoader.class);

  private static final HBaseCommonTestingUtility TEST_UTIL = new HBaseCommonTestingUtility();
  private static final Configuration conf = TEST_UTIL.getConfiguration();
  static {
    TEST_UTIL.getDataTestDir(); // prepare data test dir and hbase local dir
  }

  @Test
  public void testCleanupOldJars() throws Exception {
    String className = "TestCleanupOldJars";
    String folder = TEST_UTIL.getDataTestDir().toString();
    File jarFile = ClassLoaderTestHelper.buildJar(
      folder, className, null, ClassLoaderTestHelper.localDirPath(conf));
    File tmpJarFile = new File(jarFile.getParent(), "/tmp/" + className + ".test.jar");

    if (tmpJarFile.exists()) {
      tmpJarFile.delete();
    }

    assertFalse("tmp jar file should not exist", tmpJarFile.exists());
    ClassLoader parent = TestCoprocessorClassLoader.class.getClassLoader();
    CoprocessorClassLoader.getClassLoader(new Path(jarFile.getParent()), parent, "112", conf);
    IOUtils.copyBytes(new FileInputStream(jarFile),
      new FileOutputStream(tmpJarFile), conf, true);
    assertTrue("tmp jar file should be created", tmpJarFile.exists());
    Path path = new Path(jarFile.getAbsolutePath());
    CoprocessorClassLoader.parentDirLockSet.clear(); // So that clean up can be triggered
    ClassLoader classLoader = CoprocessorClassLoader.getClassLoader(path, parent, "111", conf);
    assertNotNull("Classloader should be created", classLoader);
    assertFalse("tmp jar file should be removed", tmpJarFile.exists());
  }

  @Test
  public void testLibJarName() throws Exception {
    checkingLibJarName("TestLibJarName.jar", "/lib/");
  }

  @Test
  public void testRelativeLibJarName() throws Exception {
    checkingLibJarName("TestRelativeLibJarName.jar", "lib/");
  }

  /**
   * Test to make sure the lib jar file extracted from a coprocessor jar have
   * the right name.  Otherwise, some existing jar could be override if there are
   * naming conflicts.
   */
  private void checkingLibJarName(String jarName, String libPrefix) throws Exception {
    File tmpFolder = new File(ClassLoaderTestHelper.localDirPath(conf), "tmp");
    if (tmpFolder.exists()) { // Clean up the tmp folder
      File[] files = tmpFolder.listFiles();
      if (files != null) {
        for (File f: files) {
          f.delete();
        }
      }
    }
    String className = "CheckingLibJarName";
    String folder = TEST_UTIL.getDataTestDir().toString();
    File innerJarFile = ClassLoaderTestHelper.buildJar(
      folder, className, null, ClassLoaderTestHelper.localDirPath(conf));
    File targetJarFile = new File(innerJarFile.getParent(), jarName);
    ClassLoaderTestHelper.addJarFilesToJar(targetJarFile, libPrefix, innerJarFile);
    Path path = new Path(targetJarFile.getAbsolutePath());
    ClassLoader parent = TestCoprocessorClassLoader.class.getClassLoader();
    ClassLoader classLoader = CoprocessorClassLoader.getClassLoader(path, parent, "112", conf);
    assertNotNull("Classloader should be created", classLoader);
    String fileToLookFor = "." + className + ".jar";
    String[] files = tmpFolder.list();
    if (files != null) {
      for (String f: files) {
        if (f.endsWith(fileToLookFor) && f.contains(jarName)) {
          // Cool, found it;
          return;
        }
      }
    }
    fail("Could not find the expected lib jar file");
  }

  // HBASE-14548
  @Test
  public void testDirectoryAndWildcard() throws Exception {
    String testClassName = "TestClass";
    String dataTestDir = TEST_UTIL.getDataTestDir().toString();
    System.out.println(dataTestDir);
    String localDirContainingJar = ClassLoaderTestHelper.localDirPath(conf);
    ClassLoaderTestHelper.buildJar(dataTestDir, testClassName, null, localDirContainingJar);
    ClassLoader parent = TestCoprocessorClassLoader.class.getClassLoader();
    CoprocessorClassLoader.parentDirLockSet.clear(); // So that clean up can be triggered

    // Directory
    Path testPath = new Path(localDirContainingJar);
    CoprocessorClassLoader coprocessorClassLoader = CoprocessorClassLoader.getClassLoader(testPath,
      parent, "113_1", conf);
    verifyCoprocessorClassLoader(coprocessorClassLoader, testClassName);

    // Wildcard - *.jar
    testPath = new Path(localDirContainingJar, "*.jar");
    coprocessorClassLoader = CoprocessorClassLoader.getClassLoader(testPath, parent, "113_2", conf);
    verifyCoprocessorClassLoader(coprocessorClassLoader, testClassName);

    // Wildcard - *.j*
    testPath = new Path(localDirContainingJar, "*.j*");
    coprocessorClassLoader = CoprocessorClassLoader.getClassLoader(testPath, parent, "113_3", conf);
    verifyCoprocessorClassLoader(coprocessorClassLoader, testClassName);
  }

  /**
   * Verify the coprocessorClassLoader is not null and the expected class can be loaded successfully
   * @param coprocessorClassLoader the CoprocessorClassLoader to verify
   * @param className the expected class to be loaded by the coprocessorClassLoader
   * @throws ClassNotFoundException if the class, which should be loaded via the
   *    coprocessorClassLoader, does not exist
   */
  private void verifyCoprocessorClassLoader(CoprocessorClassLoader coprocessorClassLoader,
      String className) throws ClassNotFoundException {
    assertNotNull("Classloader should be created and not null", coprocessorClassLoader);
    assertEquals(className, coprocessorClassLoader.loadClass(className).getName());
  }
}
