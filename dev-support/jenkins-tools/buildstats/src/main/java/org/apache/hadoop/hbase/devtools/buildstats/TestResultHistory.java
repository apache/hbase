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
package org.apache.hadoop.hbase.devtools.buildstats;

import com.offbytwo.jenkins.JenkinsServer;
import com.offbytwo.jenkins.client.JenkinsHttpClient;
import com.offbytwo.jenkins.model.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

public class TestResultHistory {
  public final static String STATUS_REGRESSION = "REGRESSION";
  public final static String STATUS_FAILED = "FAILED";
  public final static String STATUS_PASSED = "PASSED";
  public final static String STATUS_FIXED = "FIXED";
  public static int BUILD_HISTORY_NUM = 15;

  private JenkinsHttpClient client;
  private String jobName;

  public TestResultHistory(String apacheHTTPURL, String jobName, String userName, String passWord)
      throws URISyntaxException {
    this.client = new JenkinsHttpClient(new URI(apacheHTTPURL), userName, passWord);
    this.jobName = jobName;
  }

  public static void main(String[] args) {

    if (args.length < 2) {
      printUsage();
      return;
    }

    String apacheHTTPUrl = args[0];
    String jobName = args[1];
    if (args.length > 2) {
      int tmpHistoryJobNum = -1;
      try {
        tmpHistoryJobNum = Integer.parseInt(args[2]);
      } catch (NumberFormatException ex) {
        // ignore
      }
      if (tmpHistoryJobNum > 0) {
        BUILD_HISTORY_NUM = tmpHistoryJobNum;
      }
    }

    try {
      TestResultHistory buildHistory = new TestResultHistory(apacheHTTPUrl, jobName, "", "");
      HistoryReport report = buildHistory.getReport();
      // display result in console
      report.printReport();
    } catch (Exception ex) {
      System.out.println("Got unexpected exception: " + ex.getMessage());
    }
  }

  protected static void printUsage() {
    System.out.println("<Jenkins HTTP URL> <Job Name> [Number of Historical Jobs to Check]");
    System.out.println("Sample Input: \"https://builds.apache.org\" "
        + "\"HBase-TRUNK-on-Hadoop-2.0.0\" ");
  }

  public HistoryReport getReport() {
    HistoryReport report = new HistoryReport();

    List<Integer> buildWithTestResults = new ArrayList<Integer>();
    Map<String, int[]> failureStats = new HashMap<String, int[]>();

    try {
      JenkinsServer jenkins = new JenkinsServer(this.client);
      Map<String, Job> jobs = jenkins.getJobs();
      JobWithDetails job = jobs.get(jobName.toLowerCase()).details();

      // build test case failures stats for the past 10 builds
      Build lastBuild = job.getLastBuild();
      int startingBuildNumber =
          (lastBuild.getNumber() - BUILD_HISTORY_NUM > 0) ? lastBuild.getNumber()
              - BUILD_HISTORY_NUM + 1 : 1;

      Map<Integer, HashMap<String, String>> executedTestCases =
          new HashMap<Integer, HashMap<String, String>>();
      Map<Integer, Set<String>> skippedTestCases = new TreeMap<Integer, Set<String>>();
      Set<String> allExecutedTestCases = new HashSet<String>();
      Map<Integer, Set<String>> normalizedTestSet = new HashMap<Integer, Set<String>>();
      String buildUrl = lastBuild.getUrl();
      for (int i = startingBuildNumber; i <= lastBuild.getNumber(); i++) {
        HashMap<String, String> buildExecutedTestCases = new HashMap<String, String>(2048);
        String curBuildUrl = buildUrl.replaceFirst("/" + lastBuild.getNumber(), "/" + i);
        List<String> failedCases = null;
        try {
          failedCases = getBuildFailedTestCases(curBuildUrl, buildExecutedTestCases);
          buildWithTestResults.add(i);
        } catch (Exception ex) {
          // can't get result so skip it
          continue;
        }
        executedTestCases.put(i, buildExecutedTestCases);
        HashSet<String> tmpSet = new HashSet<String>();
        for (String tmpTestCase : buildExecutedTestCases.keySet()) {
          allExecutedTestCases.add(tmpTestCase.substring(0, tmpTestCase.lastIndexOf(".")));
          tmpSet.add(tmpTestCase.substring(0, tmpTestCase.lastIndexOf(".")));
        }
        normalizedTestSet.put(i, tmpSet);

        // set test result failed cases of current build
        for (String curFailedTestCase : failedCases) {
          if (failureStats.containsKey(curFailedTestCase)) {
            int[] testCaseResultArray = failureStats.get(curFailedTestCase);
            testCaseResultArray[i - startingBuildNumber] = -1;
          } else {
            int[] testResult = new int[BUILD_HISTORY_NUM];
            testResult[i - startingBuildNumber] = -1;
            // refill previous build test results for newly failed test case
            for (int k = startingBuildNumber; k < i; k++) {
              HashMap<String, String> tmpBuildExecutedTestCases = executedTestCases.get(k);
              if (tmpBuildExecutedTestCases != null
                  && tmpBuildExecutedTestCases.containsKey(curFailedTestCase)) {
                String statusStr = tmpBuildExecutedTestCases.get(curFailedTestCase);
                testResult[k - startingBuildNumber] = convertStatusStringToInt(statusStr);
              }
            }
            failureStats.put(curFailedTestCase, testResult);
          }

        }

        // set test result for previous failed test cases
        for (String curTestCase : failureStats.keySet()) {
          if (!failedCases.contains(curTestCase) && buildExecutedTestCases.containsKey(curTestCase)) {
            String statusVal = buildExecutedTestCases.get(curTestCase);
            int[] testCaseResultArray = failureStats.get(curTestCase);
            testCaseResultArray[i - startingBuildNumber] = convertStatusStringToInt(statusVal);
          }
        }
      }

      // check which test suits skipped
      for (int i = startingBuildNumber; i <= lastBuild.getNumber(); i++) {
        Set<String> skippedTests = new HashSet<String>();
        HashMap<String, String> tmpBuildExecutedTestCases = executedTestCases.get(i);
        if (tmpBuildExecutedTestCases == null || tmpBuildExecutedTestCases.isEmpty()) continue;
        // normalize test case names
        Set<String> tmpNormalizedTestCaseSet = normalizedTestSet.get(i);
        for (String testCase : allExecutedTestCases) {
          if (!tmpNormalizedTestCaseSet.contains(testCase)) {
            skippedTests.add(testCase);
          }
        }
        skippedTestCases.put(i, skippedTests);
      }

      report.setBuildsWithTestResults(buildWithTestResults);
      for (String failedTestCase : failureStats.keySet()) {
        int[] resultHistory = failureStats.get(failedTestCase);
        int[] compactHistory = new int[buildWithTestResults.size()];
        int index = 0;
        for (Integer i : buildWithTestResults) {
          compactHistory[index] = resultHistory[i - startingBuildNumber];
          index++;
        }
        failureStats.put(failedTestCase, compactHistory);
      }

      report.setHistoryResults(failureStats, skippedTestCases);

    } catch (Exception ex) {
      System.out.println(ex);
      ex.printStackTrace();
    }

    return report;
  }

  /**
   * @param statusVal
   * @return 1 means PASSED, -1 means FAILED, 0 means SKIPPED
   */
  static int convertStatusStringToInt(String statusVal) {

    if (statusVal.equalsIgnoreCase(STATUS_REGRESSION) || statusVal.equalsIgnoreCase(STATUS_FAILED)) {
      return -1;
    } else if (statusVal.equalsIgnoreCase(STATUS_PASSED)) {
      return 1;
    }

    return 0;
  }

  /**
   * Get failed test cases of a build
   * @param buildURL Jenkins build job URL
   * @param executedTestCases Set of test cases which was executed for the build
   * @return list of failed test case names
   */
  List<String> getBuildFailedTestCases(String buildURL, HashMap<String, String> executedTestCases)
      throws IOException {
    List<String> result = new ArrayList<String>();

    String apiPath =
        urlJoin(buildURL,
          "testReport?depth=10&tree=suites[cases[className,name,status,failedSince]]");

    List<TestSuite> suites = client.get(apiPath, BuildResultWithTestCaseDetails.class).getSuites();

    result = getTestSuiteFailedTestcase(suites, executedTestCases);

    return result;
  }

  private List<String> getTestSuiteFailedTestcase(List<TestSuite> suites,
      HashMap<String, String> executedTestCases) {
    List<String> result = new ArrayList<String>();

    if (suites == null) {
      return result;
    }

    for (TestSuite curTestSuite : suites) {
      for (TestCaseResult curTestCaseResult : curTestSuite.getCases()) {
        if (curTestCaseResult.getStatus().equalsIgnoreCase(STATUS_FAILED)
            || curTestCaseResult.getStatus().equalsIgnoreCase(STATUS_REGRESSION)) {
          // failed test case
          result.add(curTestCaseResult.getFullName());
        }
        executedTestCases.put(curTestCaseResult.getFullName(), curTestCaseResult.getStatus());
      }
    }

    return result;
  }

  String urlJoin(String path1, String path2) {
    if (!path1.endsWith("/")) {
      path1 += "/";
    }
    if (path2.startsWith("/")) {
      path2 = path2.substring(1);
    }
    return path1 + path2;
  }
}
