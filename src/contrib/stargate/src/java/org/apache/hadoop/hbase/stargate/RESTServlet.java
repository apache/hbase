/*
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
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.stargate;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.stargate.metrics.StargateMetrics;

/**
 * Singleton class encapsulating global REST servlet state and functions.
 */
public class RESTServlet implements Constants {

  private static RESTServlet instance;

  HBaseConfiguration conf;
  HTablePool pool;
  AtomicBoolean stopping = new AtomicBoolean(false);
  StargateMetrics metrics = new StargateMetrics();

  /**
   * @return the RESTServlet singleton instance
   * @throws IOException
   */
  public synchronized static RESTServlet getInstance() throws IOException {
    if (instance == null) {
      instance = new RESTServlet();
    }
    return instance;
  }

  /**
   * Constructor
   * @throws IOException
   */
  public RESTServlet() throws IOException {
    this.conf = new HBaseConfiguration();
    this.pool = new HTablePool(conf, 10);
  }

  HTablePool getTablePool() {
    return pool;
  }

  HBaseConfiguration getConfiguration() {
    return conf;
  }

  StargateMetrics getMetrics() {
    return metrics;
  }
}
