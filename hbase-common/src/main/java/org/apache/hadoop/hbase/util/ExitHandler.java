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

import org.apache.yetus.audience.InterfaceAudience;

/**
 * A utility class to wrap system exit calls. We use this to allow for testing and to provide a
 * consistent way to handle system exit across the codebase. NOTE: We should avoid calling
 * System.exit directly in the codebase. Instead, we should use this class to handle system exit
 * calls.
 */
@InterfaceAudience.Private
public class ExitHandler {
  private static ExitHandler instance = new ExitHandler();

  public static ExitHandler getInstance() {
    return instance;
  }

  public static void setInstance(ExitHandler handler) {
    instance = handler;
  }

  /**
   * This method is called when the system needs to exit. By default, it calls System.exit.
   * Subclasses can override this method to provide custom exit behavior.
   * @param status the exit status
   */
  @SuppressWarnings("checkstyle:RegexpSinglelineJava")
  public void exit(int status) {
    System.exit(status);
  }
}
