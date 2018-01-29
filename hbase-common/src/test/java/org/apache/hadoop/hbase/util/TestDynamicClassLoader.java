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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.File;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test TestDynamicClassLoader
 */
@Category({MiscTests.class, SmallTests.class})
public class TestDynamicClassLoader {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestDynamicClassLoader.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestDynamicClassLoader.class);

  private static final HBaseCommonTestingUtility TEST_UTIL = new HBaseCommonTestingUtility();
  private Configuration conf;

  static {
    TEST_UTIL.getConfiguration().set(
        "hbase.dynamic.jars.dir", TEST_UTIL.getDataTestDir().toString());
  }

  @Before
  public void initializeConfiguration() {
    conf = new Configuration(TEST_UTIL.getConfiguration());
  }

  @Test
  public void testLoadClassFromLocalPath() throws Exception {
    ClassLoader parent = TestDynamicClassLoader.class.getClassLoader();
    DynamicClassLoader classLoader = new DynamicClassLoader(conf, parent);

    String className = "TestLoadClassFromLocalPath";
    deleteClass(className);
    try {
      classLoader.loadClass(className);
      fail("Should not be able to load class " + className);
    } catch (ClassNotFoundException cnfe) {
      // expected, move on
    }

    try {
      String folder = TEST_UTIL.getDataTestDir().toString();
      ClassLoaderTestHelper.buildJar(
        folder, className, null, ClassLoaderTestHelper.localDirPath(conf));
      classLoader.loadClass(className);
    } catch (ClassNotFoundException cnfe) {
      LOG.error("Should be able to load class " + className, cnfe);
      fail(cnfe.getMessage());
    }
  }

  @Test
  public void testLoadClassFromAnotherPath() throws Exception {
    ClassLoader parent = TestDynamicClassLoader.class.getClassLoader();
    DynamicClassLoader classLoader = new DynamicClassLoader(conf, parent);

    String className = "TestLoadClassFromAnotherPath";
    deleteClass(className);
    try {
      classLoader.loadClass(className);
      fail("Should not be able to load class " + className);
    } catch (ClassNotFoundException cnfe) {
      // expected, move on
    }

    try {
      String folder = TEST_UTIL.getDataTestDir().toString();
      ClassLoaderTestHelper.buildJar(folder, className, null);
      classLoader.loadClass(className);
    } catch (ClassNotFoundException cnfe) {
      LOG.error("Should be able to load class " + className, cnfe);
      fail(cnfe.getMessage());
    }
  }

  @Test
  public void testLoadClassFromLocalPathWithDynamicDirOff() throws Exception {
    conf.setBoolean("hbase.use.dynamic.jars", false);
    ClassLoader parent = TestDynamicClassLoader.class.getClassLoader();
    DynamicClassLoader classLoader = new DynamicClassLoader(conf, parent);

    String className = "TestLoadClassFromLocalPath";
    deleteClass(className);

    try {
      String folder = TEST_UTIL.getDataTestDir().toString();
      ClassLoaderTestHelper.buildJar(
          folder, className, null, ClassLoaderTestHelper.localDirPath(conf));
      classLoader.loadClass(className);
      fail("Should not be able to load class " + className);
    } catch (ClassNotFoundException cnfe) {
      // expected, move on
    }
  }

  private void deleteClass(String className) throws Exception {
    String jarFileName = className + ".jar";
    File file = new File(TEST_UTIL.getDataTestDir().toString(), jarFileName);
    file.delete();
    assertFalse("Should be deleted: " + file.getPath(), file.exists());

    file = new File(conf.get("hbase.dynamic.jars.dir"), jarFileName);
    file.delete();
    assertFalse("Should be deleted: " + file.getPath(), file.exists());

    file = new File(ClassLoaderTestHelper.localDirPath(conf), jarFileName);
    file.delete();
    assertFalse("Should be deleted: " + file.getPath(), file.exists());
  }
}
