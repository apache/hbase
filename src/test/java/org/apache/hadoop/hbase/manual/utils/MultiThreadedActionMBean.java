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
package org.apache.hadoop.hbase.manual.utils;

public interface MultiThreadedActionMBean {

  /**
   * @return the average number of keys processed per second since the previous
   *         invocation of this method
   */
  public long getKeysPerSecond();

  /**
   * @return the average number of columns processed per second since the
   *         previous invocation of this method
   */
  public long getColumnsPerSecond();

  /**
   * @return the average latency of operations since the previous invocation of
   *         this method
   */
  public long getAverageLatency();

  /**
   * @return the average number of keys processed per second since the creation
   *         of this action
   */
  public long getCumulativeKeysPerSecond();

  /**
   * @return the total number of keys processed since the creation of this
   *         action
   */
  public long getCumulativeKeys();

  /**
   * @return the total number of columns processed since the creation of this
   *         action
   */
  public long getCumulativeColumns();

  /**
   * @return the average latency of operations since the creation of this action
   */
  public long getCumulativeAverageLatency();

  /**
   * @return the total number of errors since the creation of this action
   */
  public long getCumulativeErrors();

  /**
   * @return the total number of operation failures since the creation of this
   *         action
   */
  public long getCumulativeOpFailures();

  /**
   * @return the total number of keys verified since the creation of this action
   */
  public long getCumulativeKeysVerified();
}
