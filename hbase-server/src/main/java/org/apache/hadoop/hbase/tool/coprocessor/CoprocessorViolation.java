/**
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

package org.apache.hadoop.hbase.tool.coprocessor;

import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.MoreObjects;

@InterfaceAudience.Private
public class CoprocessorViolation {
  public enum Severity {
    WARNING, ERROR
  }

  private final String className;
  private final Severity severity;
  private final String message;
  private final Throwable throwable;

  public CoprocessorViolation(String className, Severity severity, String message) {
    this(className, severity, message, null);
  }

  public CoprocessorViolation(String className, Severity severity, String message,
      Throwable t) {
    this.className = className;
    this.severity = severity;
    this.message = message;
    this.throwable = t;
  }

  public String getClassName() {
    return className;
  }

  public Severity getSeverity() {
    return severity;
  }

  public String getMessage() {
    return message;
  }

  public Throwable getThrowable() {
    return throwable;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("className", className)
        .add("severity", severity)
        .add("message", message)
        .add("throwable", throwable)
        .toString();
  }
}
