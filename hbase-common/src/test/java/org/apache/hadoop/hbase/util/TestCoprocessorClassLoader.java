/*
 *
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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test TestCoprocessorClassLoader. More tests are in TestClassLoading
 */
@Category({MiscTests.class, SmallTests.class})
public class TestCoprocessorClassLoader {

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
    if (tmpJarFile.exists()) tmpJarFile.delete();
    assertFalse("tmp jar file should not exist", tmpJarFile.exists());
    IOUtils.copyBytes(new FileInputStream(jarFile),
      new FileOutputStream(tmpJarFile), conf, true);
    assertTrue("tmp jar file should be created", tmpJarFile.exists());
    Path path = new Path(jarFile.getAbsolutePath());
    ClassLoader parent = TestCoprocessorClassLoader.class.getClassLoader();
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
      for (File f: tmpFolder.listFiles()) {
        f.delete();
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
    for (String f: tmpFolder.list()) {
      if (f.endsWith(fileToLookFor) && f.contains(jarName)) {
        // Cool, found it;
        return;
      }
    }
    fail("Could not find the expected lib jar file");
  }
}
