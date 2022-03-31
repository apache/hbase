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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Thrown if the region server log directory exists (which indicates another region server is
 * running at the same address)
 */
@InterfaceAudience.Public
public class RegionServerRunningException extends IOException {
  private static final long serialVersionUID = (1L << 31) - 1L;

  /** Default Constructor */
  public RegionServerRunningException() {
    super();
  }

  /**
   * Constructs the exception and supplies a string as the message
   * @param s - message
   */
  public RegionServerRunningException(String s) {
    super(s);
  }

}
