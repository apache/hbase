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

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.hadoop.hbase.testclassification.IntegrationTests;
import org.apache.hadoop.hbase.util.AbstractHBaseTool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.platform.engine.DiscoverySelector;
import org.junit.platform.engine.discovery.DiscoverySelectors;
import org.junit.platform.launcher.Launcher;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;
import org.junit.platform.launcher.listeners.TestExecutionSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLine;

/**
 * This class drives the Integration test suite execution. Executes all tests having
 *
 * <pre>
 * &#64;Tag(IntegrationTests.TAG)
 * </pre>
 *
 * annotation against an already deployed distributed cluster.
 */
public class IntegrationTestsDriver extends AbstractHBaseTool {
  private static final String SHORT_REGEX_ARG = "r";
  private static final String LONG_REGEX_ARG = "regex";
  private static final Logger LOG = LoggerFactory.getLogger(IntegrationTestsDriver.class);
  private IntegrationTestFilter intTestFilter = new IntegrationTestFilter();

  public static void main(String[] args) throws Exception {
    int ret = ToolRunner.run(new IntegrationTestsDriver(), args);
    System.exit(ret);
  }

  private static class IntegrationTestFilter extends ClassTestFinder.TestClassFilter {
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
        !c.getName().contains("IntegrationTestingUtility") && super.isCandidateClass(c);
    }
  }

  @Override
  protected void addOptions() {
    addOptWithArg(SHORT_REGEX_ARG, LONG_REGEX_ARG,
      "Java regex to use selecting tests to run: e.g. .*TestBig.*"
        + " will select all tests that include TestBig in their name.  Default: "
        + ".*IntegrationTest.*");
  }

  @Override
  protected void processOptions(CommandLine cmd) {
    String testFilterString = cmd.getOptionValue(SHORT_REGEX_ARG);
    if (testFilterString != null) {
      intTestFilter.setPattern(testFilterString);
    }
  }

  /**
   * Returns test classes annotated with @Category(IntegrationTests.class), according to the filter
   * specific on the command line (if any).
   */
  private Class<?>[] findIntegrationTestClasses()
    throws ClassNotFoundException, LinkageError, IOException {
    ClassTestFinder.TestFileNameFilter nameFilter = new ClassTestFinder.TestFileNameFilter();
    ClassFinder classFinder = new ClassFinder(nameFilter, nameFilter, intTestFilter);
    Set<Class<?>> classes = classFinder.findClasses(true);
    return classes.toArray(new Class<?>[classes.size()]);
  }

  private static int runTests(Class<?>[] classes) {
    DiscoverySelector[] selectors = new DiscoverySelector[classes.length];
    for (int i = 0; i < classes.length; i++) {
      selectors[i] = DiscoverySelectors.selectClass(classes[i]);
    }
    LauncherDiscoveryRequest request =
      LauncherDiscoveryRequestBuilder.request().selectors(selectors).build();
    Launcher launcher = LauncherFactory.create();
    SummaryGeneratingListener listener = new SummaryGeneratingListener();
    launcher.registerTestExecutionListeners(listener);
    launcher.execute(request);

    TestExecutionSummary summary = listener.getSummary();
    summary.printTo(new PrintWriter(System.out));
    return summary.getTotalFailureCount() > 0 ? 1 : 0;
  }

  @Override
  protected int doWork() throws Exception {
    // this is called from the command line, so we should set to use the distributed cluster
    IntegrationTestingUtility.setUseDistributedCluster(conf);
    Class<?>[] classes = findIntegrationTestClasses();
    LOG.info("Found " + classes.length + " integration tests to run:");
    for (Class<?> aClass : classes) {
      LOG.info("  " + aClass);
    }
    return runTests(classes);
  }
}
