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
package org.apache.hadoop.hbase.ipc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.ServerCallable;
import org.apache.hadoop.hbase.client.coprocessor.Exec;
import org.apache.hadoop.hbase.client.coprocessor.ExecResult;
import org.apache.hadoop.hbase.util.Bytes;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

/**
 * Backs a {@link org.apache.hadoop.hbase.ipc.CoprocessorProtocol} subclass proxy and forwards method
 * invocations for server execution.  Note that internally this will issue a
 * separate RPC call for each method invocation.
 */
public class MasterExecRPCInvoker implements InvocationHandler {
  // LOG is NOT in hbase subpackage intentionally so that the default HBase
  // DEBUG log level does NOT emit RPC-level logging.
  private static final Log LOG = LogFactory.getLog("org.apache.hadoop.ipc.MasterExecRPCInvoker");

  private Configuration conf;
  private final HConnection connection;
  private Class<? extends CoprocessorProtocol> protocol;

  public MasterExecRPCInvoker(Configuration conf,
                              HConnection connection,
                              Class<? extends CoprocessorProtocol> protocol) {
    this.conf = conf;
    this.connection = connection;
    this.protocol = protocol;
  }

  @Override
  public Object invoke(Object instance, final Method method, final Object[] args)
      throws Throwable {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Call: "+method.getName()+", "+(args != null ? args.length : 0));
    }
    Exec exec = new Exec(conf, protocol, method, args);
    ExecResult result = connection.getMaster().execCoprocessor(exec);
    LOG.debug("Master Result is value="+result.getValue());
    return result.getValue();
  }
}