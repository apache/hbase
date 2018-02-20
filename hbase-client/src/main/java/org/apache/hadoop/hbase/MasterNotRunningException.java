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
package org.apache.hadoop.hbase;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Thrown if the master is not running
 */
@InterfaceAudience.Public
public class MasterNotRunningException extends HBaseIOException {
  private static final long serialVersionUID = (1L << 23) - 1L;
  /** default constructor */
  public MasterNotRunningException() {
    super();
  }

  /**
   * Constructor
   * @param s message
   */
  public MasterNotRunningException(String s) {
    super(s);
  }

  /**
   * Constructor taking another exception.
   * @param e Exception to grab data from.
   */
  public MasterNotRunningException(Exception e) {
    super(e);
  }

  public MasterNotRunningException(String s, Exception e) {
    super(s, e);
  }
}
