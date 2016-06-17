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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.log4j.AsyncAppender;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.PatternLayout;

/**
 * Logger class that buffers before trying to log to the specified console.
 */
@InterfaceAudience.Private
public class AsyncConsoleAppender extends AsyncAppender {
  private final ConsoleAppender consoleAppender;

  public AsyncConsoleAppender() {
    super();
    consoleAppender = new ConsoleAppender(new PatternLayout(
        "%d{ISO8601} %-5p [%t] %c{2}: %m%n"));
    this.addAppender(consoleAppender);
  }

  public void setTarget(String value) {
    consoleAppender.setTarget(value);
  }

  public void activateOptions() {
    consoleAppender.activateOptions();
    super.activateOptions();
  }

}
