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
import java.util.ArrayList;

import org.apache.hadoop.util.Shell.ExitCodeException;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility for executing an external script that checks the health of
 * the node. An example script can be found at
 * <tt>src/main/sh/healthcheck/healthcheck.sh</tt> in the
 * <tt>hbase-examples</tt> module.
 */
class HealthChecker {

  private static final Logger LOG = LoggerFactory.getLogger(HealthChecker.class);
  private ShellCommandExecutor shexec = null;
  private String exceptionStackTrace;

  /** Pattern used for searching in the output of the node health script */
  static private final String ERROR_PATTERN = "ERROR";

  private String healthCheckScript;
  private long scriptTimeout;

  enum HealthCheckerExitStatus {
    SUCCESS,
    TIMED_OUT,
    FAILED_WITH_EXIT_CODE,
    FAILED_WITH_EXCEPTION,
    FAILED
  }

  /**
   * Initialize.
   *
   * @param location the location of the health script
   * @param timeout the timeout to be used for the health script
   */
  public void init(String location, long timeout) {
    this.healthCheckScript = location;
    this.scriptTimeout = timeout;
    ArrayList<String> execScript = new ArrayList<>();
    execScript.add(healthCheckScript);
    this.shexec = new ShellCommandExecutor(execScript.toArray(new String[execScript.size()]), null,
        null, scriptTimeout);
    LOG.info("HealthChecker initialized with script at " + this.healthCheckScript +
      ", timeout=" + timeout);
  }

  public HealthReport checkHealth() {
    HealthCheckerExitStatus status = HealthCheckerExitStatus.SUCCESS;
    try {
      // Calling this execute leaves around running executor threads.
      shexec.execute();
    } catch (ExitCodeException e) {
      // ignore the exit code of the script
      LOG.warn("Caught exception : " + e + ",exit code:" + e.getExitCode());
      status = HealthCheckerExitStatus.FAILED_WITH_EXIT_CODE;
    } catch (IOException e) {
      LOG.warn("Caught exception : " + e);
      status = HealthCheckerExitStatus.FAILED_WITH_EXCEPTION;
      exceptionStackTrace = org.apache.hadoop.util.StringUtils.stringifyException(e);
    } finally {
      if (shexec.isTimedOut()) {
        status = HealthCheckerExitStatus.TIMED_OUT;
      }
      if (status == HealthCheckerExitStatus.SUCCESS) {
        if (hasErrors(shexec.getOutput())) {
          status = HealthCheckerExitStatus.FAILED;
        }
      }
    }
    return new HealthReport(status, getHealthReport(status));
  }

  private boolean hasErrors(String output) {
    String[] splits = output.split("\n");
    for (String split : splits) {
      if (split.startsWith(ERROR_PATTERN)) {
        return true;
      }
    }
    return false;
  }

  private String getHealthReport(HealthCheckerExitStatus status){
    String healthReport = null;
    switch (status) {
    case SUCCESS:
      healthReport = "Server is healthy.";
      break;
    case TIMED_OUT:
      healthReport = "Health script timed out";
      break;
    case FAILED_WITH_EXCEPTION:
      healthReport = exceptionStackTrace;
      break;
    case FAILED_WITH_EXIT_CODE:
      healthReport = "Health script failed with exit code.";
      break;
    case FAILED:
      healthReport = shexec.getOutput();
      break;
    }
    return healthReport;
  }
}
