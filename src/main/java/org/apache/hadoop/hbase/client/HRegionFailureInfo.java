/**
 * Copyright 2010 The Apache Software Foundation
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

package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class to maintain the errors encountered while performing HBase operations at per region level on the client side.
 * It has information about:
 * 1. region name
 * 2. server name
 * 3. exceptions
 */

public class HRegionFailureInfo implements Serializable {
  private String serverName;
  private String regionName;

  // list of exceptions encountered at RegionServer level
  private List<Throwable> exceptions = new ArrayList<Throwable>();

  public HRegionFailureInfo(final String regionName) {
    this.regionName = regionName;
  }

  public HRegionFailureInfo(final String serverName, final String regionName) {
    this.regionName = regionName;
    this.serverName = serverName;
  }

  public void setServerName(final String name) {
    this.serverName = name;
  }

  public void addException(final Throwable ex) {
    exceptions.add(ex);
  }

  public void addAllExceptions(final List<Throwable> ex) {
    exceptions.addAll(ex);
  }

  public final String getServerName() {
    return this.serverName;
  }

  public final String getRegionName() {
    return this.regionName;
  }

  public final List<Throwable> getExceptions() {
    return this.exceptions;
  }

  public String toString() {
    StringBuilder buffer = new StringBuilder("");
    buffer.append("Region: " + regionName + "\n");
    buffer.append("Server: " + this.serverName + "\n");
    buffer.append("Exceptions: \n");
    for (Throwable e : exceptions) {
      if (e == null) {
        continue;
      }
      buffer.append(e.toString());

      StringWriter errors = new StringWriter();
      e.printStackTrace(new PrintWriter(errors));
      buffer.append(errors.toString());
      buffer.append("\n");

      try {
        errors.close();
      } catch (IOException ex) {} // ignore
    }

    return buffer.toString();
  }

  public boolean equals(final HRegionFailureInfo h) {
    return this.regionName.equals(h.getRegionName());
  }
}
