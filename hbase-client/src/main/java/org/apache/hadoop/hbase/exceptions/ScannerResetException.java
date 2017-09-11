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

package org.apache.hadoop.hbase.exceptions;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Thrown when the server side has received an Exception, and asks the Client to reset the scanner
 * state by closing the current region scanner, and reopening from the start of last seen row.
 */
@InterfaceAudience.Public
public class ScannerResetException extends DoNotRetryIOException {
  private static final long serialVersionUID = -5649728171144849619L;

  /** constructor */
  public ScannerResetException() {
    super();
  }

  /**
   * Constructor
   * @param s message
   */
  public ScannerResetException(String s) {
    super(s);
  }

  public ScannerResetException(String s, Exception e) {
    super(s, e);
  }
}
