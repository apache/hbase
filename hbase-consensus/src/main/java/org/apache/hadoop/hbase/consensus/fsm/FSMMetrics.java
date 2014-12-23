package org.apache.hadoop.hbase.consensus.fsm;

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


import org.apache.hadoop.hbase.metrics.MetricsBase;
import org.weakref.jmx.MBeanExporter;

import java.util.Collections;

public class FSMMetrics extends MetricsBase {
  /** Type string used when exporting an MBean for these metrics */
  public static final String TYPE = "FSMMetrics";
  /** Domain string used when exporting an MBean for these metrics */
  public static final String DOMAIN = "org.apache.hadoop.hbase.consensus.fsm";

  String name;
  String procId;

  public FSMMetrics(final String name, final String procId,
    final MBeanExporter exporter) {
      super(DOMAIN, TYPE, name, procId, Collections.<String, String>emptyMap(),
        exporter);
      this.name = name;
      this.procId = procId;
  }
}
