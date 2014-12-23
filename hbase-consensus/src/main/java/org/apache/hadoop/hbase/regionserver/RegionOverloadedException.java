package org.apache.hadoop.hbase.regionserver;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import java.util.List;

import org.apache.hadoop.hbase.RegionException;

public class RegionOverloadedException extends RegionException {
  private static final long serialVersionUID = -8436877560512061623L;

  /** default constructor */
  public RegionOverloadedException() {
    super();
  }

  /** @param s message
   *  @param waitMillis -- request client to backoff for waitMillis
   */
  public RegionOverloadedException(String s, long waitMillis) {
    super(s, waitMillis);
  }

  /**
   * Create a RegionOverloadedException from another one, attaching a set of related exceptions
   * from a batch operation. The new exception reuses the original exception's stack trace.
   *  
   * @param roe the original exception
   * @param exceptions other exceptions that happened in the same batch operation
   * @param waitMillis remaining time for the client to wait in milliseconds
   * @return the new exception with complete information
   */
  public static RegionOverloadedException create(RegionOverloadedException roe,
      List<Throwable> exceptions, int waitMillis) {
    StringBuilder sb = new StringBuilder(roe.getMessage());
    for (Throwable t : exceptions) {
      if (t != roe) {
        sb.append(t.toString());
        sb.append("\n");
      }
    }
    RegionOverloadedException e = new RegionOverloadedException(sb.toString(), waitMillis);
    if (roe != null) {  // Safety check
      e.setStackTrace(roe.getStackTrace());
    }
    return e;
  }

}
