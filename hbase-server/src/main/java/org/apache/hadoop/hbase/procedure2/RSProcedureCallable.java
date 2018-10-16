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
package org.apache.hadoop.hbase.procedure2;

import java.util.concurrent.Callable;

import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A general interface for a sub procedure runs at RS side.
 */
@InterfaceAudience.Private
public interface RSProcedureCallable extends Callable<Void> {

  /**
   * Initialize the callable
   * @param parameter the parameter passed from master.
   * @param rs the regionserver instance
   */
  void init(byte[] parameter, HRegionServer rs);

  /**
   * Event type used to select thread pool.
   */
  EventType getEventType();
}
