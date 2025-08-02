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
package org.apache.hadoop.hbase.util;

/**
 * Class for intercepting ExitHandler.getInstance().exit(int) calls. Use for testing main methods
 * with ExitHandler.getInstance().exit(int). Usage:
 *
 * <pre>
 * ExitHandler originalHandler = ExitHandler.getInstance();
 * LauncherExitHandler launcherExitHandler = new LauncherExitHandler();
 * ExitHandler.setInstance(launcherExitHandler);
 * try {
 *   CellCounter.main(args);
 *   fail("should be exception");
 * } catch (SecurityException e) {
 *   assertEquals(expectedExitCode, launcherExitHandler.getExitCode());
 * }
 * ExitHandler.setInstance(originalHandler);
 * </pre>
 */
public class LauncherExitHandler extends ExitHandler {
  private int exitCode;

  public LauncherExitHandler() {
    reset();
  }

  @Override
  public void exit(int status) throws SecurityException {
    this.exitCode = status;
    throw new SecurityException("Intercepted System.exit(" + status + ")");
  }

  public int getExitCode() {
    return exitCode;
  }

  public void reset() {
    exitCode = 0;
  }

}
