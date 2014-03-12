/**
 * Copyright 2014 The Apache Software Foundation
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

import java.util.HashSet;
import java.util.Set;

import org.junit.runner.Description;
import org.junit.runner.Runner;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;

/**
 * A TagRunner is used to control the running JUnit testcases by tags.
 * To use a <code>TagRunner</code>:
 * <ol>
 * <li>Annotate the testing class with <code>@RunWith(TagRunner.class)</code></li>
 * <li>Annotate the a method with <code>@TestTag({"tag"})</code></li>
 * <li>Now you can specify the system property <code>testskip</code> to skip all
 * the test cases with some tags.</li>
 * </ol>
 *
 * <code>testskip</code> is a semicolon separated string contains all the tags
 * to be skipped. To specify a system property under Maven is like this
 * <code> mvn test -Dtestskip=tag1;tag2</code>
 *
 * <code>all</code> is a special tag which will trigger all tags to be skipped.
 */
public class TagRunner extends Runner {
  private static final Set<String> skipTags = new HashSet<String>();
  private static boolean skipAll = false;
  /**
   * The system property setting skipping tags. The property is a semicolon
   * separated string.
   */
  private static String PROP_TESTSKIP = "testskip";
  /**
   * A special tags hitting all tags.
   */
  private static String TAG_ALL = "all";
  static {
    String tagStr = System.getProperty(PROP_TESTSKIP, "");
    if (tagStr.length() > 0) {
      for (String tag : tagStr.split(";")) {
        skipTags.add(tag);
      }
    }
    skipAll = skipTags.contains(TAG_ALL);
  }

  static private boolean shouldSkip(String[] tags) {
    if (skipAll) {
      return true;
    }

    for (String tag: tags) {
      if (skipTags.contains(tag)) {
        return true;
      }
    }
    return false;
  }

  private static class SelectiveRunner extends BlockJUnit4ClassRunner {
    @Override
    protected void runChild(FrameworkMethod mth, RunNotifier rn) {
      TestTag tag = mth.getAnnotation(TestTag.class);
      if (tag != null && tag.value().length > 0) {
        if (shouldSkip(tag.value())) {
          rn.fireTestIgnored(this.describeChild(mth));
          return;
        }
      }

      super.runChild(mth, rn);
    }

    public SelectiveRunner(Class<?> klass) throws InitializationError {
      super(klass);
    }
  }

  private Runner runner;

  public TagRunner(Class<?> testClass) throws InitializationError {
    this.runner = new SelectiveRunner(testClass);
  }

  @Override
  public Description getDescription() {
    return runner.getDescription();
  }

  @Override
  public void run(RunNotifier rn) {
    runner.run(rn);
  }

}
