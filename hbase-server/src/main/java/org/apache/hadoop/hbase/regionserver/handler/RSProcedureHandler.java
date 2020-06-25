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
package org.apache.hadoop.hbase.regionserver.handler;

import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.procedure2.RSProcedureCallable;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A event handler for running procedure.
 */
@InterfaceAudience.Private
public class RSProcedureHandler extends EventHandler {

  private static final Logger LOG = LoggerFactory.getLogger(RSProcedureHandler.class);

  private final long procId;

  private final RSProcedureCallable callable;

  public RSProcedureHandler(HRegionServer rs, long procId, RSProcedureCallable callable) {
    super(rs, callable.getEventType());
    this.procId = procId;
    this.callable = callable;
  }

  @Override
  public void process() {
    Throwable error = null;
    try {
      callable.call();
    } catch (Throwable t) {
      LOG.error("pid=" + this.procId, t);
      error = t;
    } finally {
      ((HRegionServer) server).remoteProcedureComplete(procId, error);
    }
  }
}
