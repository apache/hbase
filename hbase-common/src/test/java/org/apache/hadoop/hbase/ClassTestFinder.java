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

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.ClassFinder.ClassFilter;
import org.apache.hadoop.hbase.ClassFinder.FileNameFilter;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.Suite;

/**
 * ClassFinder that is pre-configured with filters that will only allow test classes.
 * The name is strange because a logical name would start with "Test" and be confusing.
 */
public class ClassTestFinder extends ClassFinder {

  public ClassTestFinder() {
    super(new TestFileNameFilter(), new TestFileNameFilter(), new TestClassFilter());
  }

  public ClassTestFinder(Class<?> category) {
    super(new TestFileNameFilter(), new TestFileNameFilter(), new TestClassFilter(category));
  }

  public static Class<?>[] getCategoryAnnotations(Class<?> c) {
    Category category = c.getAnnotation(Category.class);
    if (category != null) {
      return category.value();
    }
    return new Class<?>[0];
  }

  /** Filters both test classes and anything in the hadoop-compat modules */
  public static class TestFileNameFilter implements FileNameFilter, ResourcePathFilter {
    private static final Pattern hadoopCompactRe =
        Pattern.compile("hbase-hadoop\\d?-compat");

    @Override
    public boolean isCandidateFile(String fileName, String absFilePath) {
      boolean isTestFile = fileName.startsWith("Test")
          || fileName.startsWith("IntegrationTest");
      return isTestFile && !hadoopCompactRe.matcher(absFilePath).find();
    }

    @Override
    public boolean isCandidatePath(String resourcePath, boolean isJar) {
      return !hadoopCompactRe.matcher(resourcePath).find();
    }
  };

  /*
  * A class is considered as a test class if:
   *  - it's not Abstract AND
   *  - one or more of its methods is annotated with org.junit.Test OR
   *  - the class is annotated with Suite.SuiteClasses
  * */
  public static class TestClassFilter implements ClassFilter {
    private Class<?> categoryAnnotation = null;
    public TestClassFilter(Class<?> categoryAnnotation) {
      this.categoryAnnotation = categoryAnnotation;
    }

    public TestClassFilter() {
      this(null);
    }

    @Override
    public boolean isCandidateClass(Class<?> c) {
      return isTestClass(c) && isCategorizedClass(c);
    }

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

    private boolean isCategorizedClass(Class<?> c) {
      if (this.categoryAnnotation == null) {
        return true;
      }
      for (Class<?> cc : getCategoryAnnotations(c)) {
        if (cc.equals(this.categoryAnnotation)) {
          return true;
        }
      }
      return false;
    }
  };
};
