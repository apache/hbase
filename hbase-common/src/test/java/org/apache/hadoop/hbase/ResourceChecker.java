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

package org.apache.hadoop.hbase;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Utility class to check the resources:
 *  - log them before and after each test method
 *  - check them against a minimum or maximum
 *  - check that they don't leak during the test
 */
public class ResourceChecker {
  private static final Log LOG = LogFactory.getLog(ResourceChecker.class);
  private String tagLine;

  enum Phase {
    INITIAL, INTERMEDIATE, END
  }

  /**
   * Constructor
   * @param tagLine - the tagLine is added to the logs. Must be be null.
   */
  public ResourceChecker(final String tagLine) {
    this.tagLine = tagLine;
  }


  /**
   * Class to implement for each type of resource.
   */
  abstract static class ResourceAnalyzer {
    /**
     * Maximum we set for the resource. Will get a warning in logs
     * if we go over this limit.
     */
    public int getMax() {
      return Integer.MAX_VALUE;
    }

    /**
     * Minimum we set for the resource. Will get a warning in logs
     * if we go under this limit.
     */
    public int getMin() {
      return Integer.MIN_VALUE;
    }

    /**
     * Name of the resource analyzed. By default extracted from the class name, but
     *  can be overridden by the subclasses.
     */
    public String getName() {
      String className = this.getClass().getSimpleName();
      final String extName = ResourceAnalyzer.class.getSimpleName();
      if (className.endsWith(extName)) {
        return className.substring(0, className.length() - extName.length());
      } else {
        return className;
      }
    }

    /**
     * The value for the resource.
     * @param phase
     */
    abstract public int getVal(Phase phase);
    
    /*
     * Retrieves List of Strings which would be logged in logEndings()
     */
    public List<String> getStringsToLog() { return null; }
  }

  private List<ResourceAnalyzer> ras = new ArrayList<ResourceAnalyzer>();
  private int[] initialValues;
  private int[] endingValues;


  private void fillInit() {
    initialValues = new int[ras.size()];
    fill(Phase.INITIAL, initialValues);
  }

  private void fillEndings() {
    endingValues = new int[ras.size()];
    fill(Phase.END, endingValues);
  }

  private void fill(Phase phase, int[] vals) {
    int i = 0;
    for (ResourceAnalyzer ra : ras) {
      vals[i++] = ra.getVal(phase);
    }
  }

  public void checkInit() {
    check(initialValues);
  }

  private void checkEndings() {
    check(endingValues);
  }

  private void check(int[] vals) {
    int i = 0;
    for (ResourceAnalyzer ra : ras) {
      int cur = vals[i++];
      if (cur < ra.getMin()) {
        LOG.warn(ra.getName() + "=" + cur + " is inferior to " + ra.getMin());
      }
      if (cur > ra.getMax()) {
        LOG.warn(ra.getName() + "=" + cur + " is superior to " + ra.getMax());
      }
    }
  }

  private void logInit() {
    int i = 0;
    StringBuilder sb = new StringBuilder();
    for (ResourceAnalyzer ra : ras) {
      int cur = initialValues[i++];
      if (sb.length() > 0) sb.append(", ");
      sb.append(ra.getName()).append("=").append(cur);
    }
    LOG.info("before: " + tagLine + " " + sb);
  }

  private void logEndings() {
    assert initialValues.length == ras.size();
    assert endingValues.length == ras.size();

    int i = 0;
    StringBuilder sb = new StringBuilder();
    for (ResourceAnalyzer ra : ras) {
      int curP = initialValues[i];
      int curN = endingValues[i++];
      if (sb.length() > 0) sb.append(", ");
      sb.append(ra.getName()).append("=").append(curN).append(" (was ").append(curP).append(")");
      if (curN > curP) {
        List<String> strings = ra.getStringsToLog();
        if (strings != null) {
          for (String s : strings) {
            sb.append(s);
          }
        }
        sb.append(" - ").append(ra.getName()).append(" LEAK? -");
      }
    }
    LOG.info("after: " + tagLine + " " + sb);
  }


  /**
   * To be called as the beginning of a test method:
   * - measure the resources
   * - check vs. the limits.
   * - logs them.
   */
  public void start() {
    if (ras.size() == 0) {
      LOG.info("No resource analyzer");
      return;
    }
    fillInit();
    logInit();
    checkInit();
  }

  /**
   * To be called as the end of a test method:
   * - measure the resources
   * - check vs. the limits.
   * - check vs. the initial state
   * - logs them.
   */
  public void end() {
    if (ras.size() == 0) {
      LOG.info("No resource analyzer");
      return;
    }
    if (initialValues == null) {
      LOG.warn("No initial values");
      return;
    }

    fillEndings();
    logEndings();
    checkEndings();
  }

  /**
   * Adds a resource analyzer to the resources checked.
   */
  public void addResourceAnalyzer(ResourceAnalyzer ra) {
    ras.add(ra);
  }
}
