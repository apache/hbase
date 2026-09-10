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
package org.apache.hadoop.hbase;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Tag;

/**
 * ClassFinder that is pre-configured with filters that will only allow test classes. The name is
 * strange because a logical name would start with "Test" and be confusing.
 */
public class ClassTestFinder extends ClassFinder {

  public ClassTestFinder() {
    super(new TestFileNameFilter(), new TestFileNameFilter(), new TestClassFilter());
  }

  public ClassTestFinder(Class<?> tag) {
    super(new TestFileNameFilter(), new TestFileNameFilter(), new TestClassFilter(tag));
  }

  public static Class<?>[] getTagAnnotations(Class<?> c) {
    // TODO handle optional Tags annotation
    Tag[] tags = c.getAnnotationsByType(Tag.class);
    List<Class<?>> values = new ArrayList<>();
    if (tags != null) {
      for (Tag tag : tags) {
        try {
          values.add(Class.forName(tag.value()));
        } catch (ClassNotFoundException e) {
          throw new RuntimeException(e);
        }
      }
    }
    return values.toArray(new Class<?>[values.size()]);
  }

  /** Filters both test classes and anything in the hadoop-compat modules */
  public static class TestFileNameFilter implements FileNameFilter, ResourcePathFilter {
    private static final Pattern hadoopCompactRe = Pattern.compile("hbase-hadoop\\d?-compat");

    @Override
    public boolean isCandidateFile(String fileName, String absFilePath) {
      boolean isTestFile = fileName.startsWith("Test") || fileName.startsWith("IntegrationTest");
      return isTestFile && !hadoopCompactRe.matcher(absFilePath).find();
    }

    @Override
    public boolean isCandidatePath(String resourcePath, boolean isJar) {
      return !hadoopCompactRe.matcher(resourcePath).find();
    }
  }

  /*
   * A class is considered as a test class if: - it's not Abstract AND - one or more of its methods
   * is annotated with org.junit.Test OR - the class is annotated with Suite.SuiteClasses
   */
  public static class TestClassFilter implements ClassFilter {
    private Class<?> tagAnnotation = null;

    public TestClassFilter(Class<?> categoryAnnotation) {
      this.tagAnnotation = categoryAnnotation;
    }

    public TestClassFilter() {
      this(null);
    }

    @Override
    public boolean isCandidateClass(Class<?> c) {
      return isTestClass(c) && isTagedClass(c);
    }

    private boolean isTestClass(Class<?> c) {
      if (Modifier.isAbstract(c.getModifiers())) {
        return false;
      }

      for (Method met : c.getMethods()) {
        if (met.getAnnotation(org.junit.jupiter.api.Test.class) != null) {
          return true;
        }
      }

      return false;
    }

    private boolean isTagedClass(Class<?> c) {
      if (this.tagAnnotation == null) {
        return true;
      }
      for (Class<?> cc : getTagAnnotations(c)) {
        if (cc.equals(this.tagAnnotation)) {
          return true;
        }
      }
      return false;
    }
  }
}
