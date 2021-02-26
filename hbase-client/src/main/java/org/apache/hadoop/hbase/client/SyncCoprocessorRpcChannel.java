/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.client;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

import java.io.IOException;

import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;

/**
 * Base class which provides clients with an RPC connection to
 * call coprocessor endpoint {@link com.google.protobuf.Service}s.
 * Note that clients should not use this class directly, except through
 * {@link org.apache.hadoop.hbase.client.Table#coprocessorService(byte[])}.
 */
@InterfaceAudience.Public
abstract class SyncCoprocessorRpcChannel implements CoprocessorRpcChannel {
  private static final Logger LOG = LoggerFactory.getLogger(SyncCoprocessorRpcChannel.class);

  @Override
  @InterfaceAudience.Private
  public void callMethod(Descriptors.MethodDescriptor method,
                         RpcController controller,
                         Message request, Message responsePrototype,
                         RpcCallback<Message> callback) {
    Message response = null;
    try {
      response = callExecService(controller, method, request, responsePrototype);
    } catch (IOException ioe) {
      LOG.warn("Call failed on IOException", ioe);
      CoprocessorRpcUtils.setControllerException(controller, ioe);
    }
    if (callback != null) {
      callback.run(response);
    }
  }

  @Override
  @InterfaceAudience.Private
  public Message callBlockingMethod(Descriptors.MethodDescriptor method,
                                    RpcController controller,
                                    Message request, Message responsePrototype)
      throws ServiceException {
    try {
      return callExecService(controller, method, request, responsePrototype);
    } catch (IOException ioe) {
      throw new ServiceException("Error calling method "+method.getFullName(), ioe);
    }
  }

  protected abstract Message callExecService(RpcController controller,
      Descriptors.MethodDescriptor method, Message request, Message responsePrototype)
          throws IOException;
}
