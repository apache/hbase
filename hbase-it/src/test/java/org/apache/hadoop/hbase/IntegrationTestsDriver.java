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

import java.io.IOException;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.testclassification.IntegrationTests;
import org.apache.hadoop.hbase.util.AbstractHBaseTool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.internal.TextListener;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;

/**
 * This class drives the Integration test suite execution. Executes all
 * tests having @Category(IntegrationTests.class) annotation against an
 * already deployed distributed cluster.
 */
public class IntegrationTestsDriver extends AbstractHBaseTool {
  private static final String SHORT_REGEX_ARG = "r";
  private static final String LONG_REGEX_ARG = "regex";
  private static final Log LOG = LogFactory.getLog(IntegrationTestsDriver.class);
  private IntegrationTestFilter intTestFilter = new IntegrationTestFilter();

  public static void main(String[] args) throws Exception {
    int ret = ToolRunner.run(new IntegrationTestsDriver(), args);
    System.exit(ret);
  }

  private class IntegrationTestFilter extends ClassTestFinder.TestClassFilter {
    private Pattern testFilterRe = Pattern.compile(".*\\.IntegrationTest.*");
    public IntegrationTestFilter() {
      super(IntegrationTests.class);
    }

    public void setPattern(String pattern) {
      testFilterRe = Pattern.compile(pattern);
    }

    @Override
    public boolean isCandidateClass(Class<?> c) {
      return testFilterRe.matcher(c.getName()).find() &&
        // Our pattern will match the below NON-IntegrationTest. Rather than
        // do exotic regex, just filter it out here
        !c.getName().contains("IntegrationTestingUtility") &&
        super.isCandidateClass(c);
    }
  }

  @Override
  protected void addOptions() {
    addOptWithArg(SHORT_REGEX_ARG, LONG_REGEX_ARG,
      "Java regex to use selecting tests to run: e.g. .*TestBig.*" +
      " will select all tests that include TestBig in their name.  Default: " +
      ".*IntegrationTest.*");
  }

  @Override
  protected void processOptions(CommandLine cmd) {
    String testFilterString = cmd.getOptionValue(SHORT_REGEX_ARG, null);
    if (testFilterString != null) {
      intTestFilter.setPattern(testFilterString);
    }
  }

  /**
   * Returns test classes annotated with @Category(IntegrationTests.class),
   * according to the filter specific on the command line (if any).
   */
  private Class<?>[] findIntegrationTestClasses()
    throws ClassNotFoundException, LinkageError, IOException {
    ClassTestFinder.TestFileNameFilter nameFilter = new ClassTestFinder.TestFileNameFilter();
    ClassFinder classFinder = new ClassFinder(nameFilter, nameFilter, intTestFilter);
    Set<Class<?>> classes = classFinder.findClasses(true);
    return classes.toArray(new Class<?>[classes.size()]);
  }


  @Override
  protected int doWork() throws Exception {
    //this is called from the command line, so we should set to use the distributed cluster
    IntegrationTestingUtility.setUseDistributedCluster(conf);
    Class<?>[] classes = findIntegrationTestClasses();
    LOG.info("Found " + classes.length + " integration tests to run:");
    for (Class<?> aClass : classes) {
      LOG.info("  " + aClass);
    }
    JUnitCore junit = new JUnitCore();
    junit.addListener(new TextListener(System.out));
    Result result = junit.run(classes);

    return result.wasSuccessful() ? 0 : 1;
  }
}
