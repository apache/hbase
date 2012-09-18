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

package org.apache.hadoop.hbase;

import static junit.framework.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.regex.Pattern;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.Suite;


/**
 * Checks tests are categorized.
 */
@Category(SmallTests.class)
public class TestCheckTestClasses {

  private FileFilter TEST_CLASS_FILE_FILTER = new FileFilter() {
    @Override
    public boolean accept(File file) {
      return file.isDirectory() || isTestClassFile(file);

    }
    private boolean isTestClassFile(File file) {
      String fileName = file.getName();
      return fileName.endsWith(".class")
          && (fileName.startsWith("Test") || fileName.startsWith("IntegrationTest"));
    }
  };

  /**
   * Throws an assertion if we find a test class without category (small/medium/large/integration).
   * List all the test classes without category in the assertion message.
   */
  @Test
  public void checkClasses() throws Exception {
    List<Class<?>> badClasses = new java.util.ArrayList<Class<?>>();

    for (Class<?> c : findTestClasses()) {
      if (!existCategoryAnnotation(c, null)) {
        badClasses.add(c);
      }
    }

    assertTrue("There are " + badClasses.size() + " test classes without category: "
      + badClasses, badClasses.isEmpty());
  }

  /** Returns whether the class has @Category annotation having the xface value.
   */
  private boolean existCategoryAnnotation(Class<?> c, Class<?> xface) {
    Category category = c.getAnnotation(Category.class);

    if (category != null) {
      if (xface == null) {
        return true;
      }
      for (Class<?> cc : category.value()) {
        if (cc.equals(xface)) {
          return true;
        }
      }
    }
    return false;
  }

  /*
  * A class is considered as a test class if:
   *  - it's not Abstract AND
   *  - one or more of its methods is annotated with org.junit.Test OR
   *  - the class is annotated with Suite.SuiteClasses
  * */
  private boolean isTestClass(Class<?> c) {
    if (Modifier.isAbstract(c.getModifiers())) {
      return false;
    }

    if (c.getAnnotation(Suite.SuiteClasses.class) != null) {
      return true;
    }

    for (Method met : c.getMethods()) {
      if (met.getAnnotation(Test.class) != null) {
        return true;
      }
    }

    return false;
  }

  /**
   * Finds test classes which are annotated with @Category having xface value
   * @param xface the @Category value
   */
  public List<Class<?>> findTestClasses(Class<?> xface) throws ClassNotFoundException, IOException {
    List<Class<?>> classes = new ArrayList<Class<?>>();
    for (Class<?> c : findTestClasses()) {
      if (existCategoryAnnotation(c, xface)) {
        classes.add(c);
      }
    }
    return classes;
  }

  private List<Class<?>> findTestClasses() throws ClassNotFoundException, IOException {
    final String packageName = "org.apache.hadoop.hbase";
    final String path = packageName.replace('.', '/');

    Enumeration<URL> resources = this.getClass().getClassLoader().getResources(path);
    List<File> dirs = new ArrayList<File>();

    while (resources.hasMoreElements()) {
      URL resource = resources.nextElement();
      dirs.add(new File(resource.getFile()));
    }

    List<Class<?>> classes = new ArrayList<Class<?>>();
    for (File directory : dirs) {
      classes.addAll(findTestClasses(directory, packageName));
    }

    return classes;
  }


  private List<Class<?>> findTestClasses(File baseDirectory, String packageName)
    throws ClassNotFoundException {
    List<Class<?>> classes = new ArrayList<Class<?>>();
    if (!baseDirectory.exists()) {
      return classes;
    }

    File[] files = baseDirectory.listFiles(TEST_CLASS_FILE_FILTER);
    assertNotNull(files);
    Pattern p = Pattern.compile("hbase-hadoop\\d?-compat");
    for (File file : files) {
      final String fileName = file.getName();
      if (p.matcher(file.getAbsolutePath()).find()) {
        continue;
      }

      if (file.isDirectory()) {
        classes.addAll(findTestClasses(file, packageName + "." + fileName));
      } else {
        Class<?> c = Class.forName(
          packageName + '.' + fileName.substring(0, fileName.length() - 6),
          false,
          this.getClass().getClassLoader());

        if (isTestClass(c)) {
          classes.add(c);
        }
      }
    }

    return classes;
  }
}
