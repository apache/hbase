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
package org.apache.hadoop.hbase.logging;

import java.util.concurrent.atomic.LongAdder;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Collect the log event count.
 */
@InterfaceAudience.Private
public class EventCounter {

  private static final LongAdder INFO = new LongAdder();

  private static final LongAdder WARN = new LongAdder();

  private static final LongAdder ERROR = new LongAdder();

  private static final LongAdder FATAL = new LongAdder();

  static void info() {
    INFO.increment();
  }

  static void warn() {
    WARN.increment();
  }

  static void error() {
    ERROR.increment();
  }

  static void fatal() {
    FATAL.increment();
  }

  public static long getInfo() {
    return INFO.sum();
  }

  public static long getWarn() {
    return WARN.sum();
  }

  public static long getError() {
    return ERROR.sum();
  }

  public static long getFatal() {
    return FATAL.sum();
  }
}
