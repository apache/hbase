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
package org.apache.hadoop.hbase.thrift;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.io.IOException;

/**
 * Run ThriftServer with passed arguments. Access the exception thrown after we complete run -- if
 * an exception thrown -- via {@link #getRunException()}}. Call close to shutdown this Runner
 * and hosted {@link ThriftServer}.
 */
class ThriftServerRunner extends Thread implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(ThriftServerRunner.class);
  Exception exception = null;
  private final ThriftServer thriftServer;
  private final String [] args;

  ThriftServerRunner(ThriftServer thriftServer, String [] args) {
    this.thriftServer = thriftServer;
    this.args = args;
    LOG.info("thriftServer={}, args={}", getThriftServer(), args);
  }

  ThriftServer getThriftServer() {
    return this.thriftServer;
  }

  /**
   * @return Empty unless {@link #run()} threw an exception; if it did, access it here.
   */
  Exception getRunException() {
    return this.exception;
  }

  @Override public void run() {
    try {
      this.thriftServer.run(this.args);
    } catch (Exception e) {
      LOG.error("Run threw an exception", e);
      this.exception = e;
    }
  }

  @Override public void close() throws IOException {
    LOG.info("Stopping {}", this);
    this.thriftServer.stop();
  }
}
