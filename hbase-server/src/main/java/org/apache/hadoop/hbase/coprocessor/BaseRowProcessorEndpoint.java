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
package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.protobuf.generated.RowProcessorProtos.ProcessRequest;
import org.apache.hadoop.hbase.protobuf.generated.RowProcessorProtos.ProcessResponse;
import org.apache.hadoop.hbase.protobuf.generated.RowProcessorProtos.RowProcessorService;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RowProcessor;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

/**
 * This class demonstrates how to implement atomic read-modify-writes
 * using {@link Region#processRowsWithLocks} and Coprocessor endpoints.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public abstract class BaseRowProcessorEndpoint<S extends Message, T extends Message> 
extends RowProcessorService implements CoprocessorService, Coprocessor {
  private RegionCoprocessorEnvironment env;
  /**
   * Pass a processor to region to process multiple rows atomically.
   * 
   * The RowProcessor implementations should be the inner classes of your
   * RowProcessorEndpoint. This way the RowProcessor can be class-loaded with
   * the Coprocessor endpoint together.
   *
   * See {@code TestRowProcessorEndpoint} for example.
   *
   * The request contains information for constructing processor 
   * (see {@link #constructRowProcessorFromRequest}. The processor object defines
   * the read-modify-write procedure.
   */
  @Override
  public void process(RpcController controller, ProcessRequest request,
      RpcCallback<ProcessResponse> done) {
    ProcessResponse resultProto = null;
    try {
      RowProcessor<S,T> processor = constructRowProcessorFromRequest(request);
      Region region = env.getRegion();
      long nonceGroup = request.hasNonceGroup() ? request.getNonceGroup() : HConstants.NO_NONCE;
      long nonce = request.hasNonce() ? request.getNonce() : HConstants.NO_NONCE;
      region.processRowsWithLocks(processor, nonceGroup, nonce);
      T result = processor.getResult();
      ProcessResponse.Builder b = ProcessResponse.newBuilder();
      b.setRowProcessorResult(result.toByteString());
      resultProto = b.build();
    } catch (Exception e) {
      ResponseConverter.setControllerException(controller, new IOException(e));
    }
    done.run(resultProto);
  }

  @Override
  public Service getService() {
    return this;
  }

  /**
   * Stores a reference to the coprocessor environment provided by the
   * {@link org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost} from the region where this
   * coprocessor is loaded.  Since this is a coprocessor endpoint, it always expects to be loaded
   * on a table region, so always expects this to be an instance of
   * {@link RegionCoprocessorEnvironment}.
   * @param env the environment provided by the coprocessor host
   * @throws IOException if the provided environment is not an instance of
   * {@code RegionCoprocessorEnvironment}
   */
  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    if (env instanceof RegionCoprocessorEnvironment) {
      this.env = (RegionCoprocessorEnvironment)env;
    } else {
      throw new CoprocessorException("Must be loaded on a table region!");
    }
  }

  @Override
  public void stop(CoprocessorEnvironment env) throws IOException {
    // nothing to do
  }

  @SuppressWarnings("unchecked")
  RowProcessor<S,T> constructRowProcessorFromRequest(ProcessRequest request)
      throws IOException {
    String className = request.getRowProcessorClassName();
    Class<?> cls;
    try {
      cls = Class.forName(className);
      RowProcessor<S,T> ci = (RowProcessor<S,T>) cls.newInstance();
      if (request.hasRowProcessorInitializerMessageName()) {
        Class<?> imn = Class.forName(request.getRowProcessorInitializerMessageName())
            .asSubclass(Message.class);
        Method m;
        try {
          m = imn.getMethod("parseFrom", ByteString.class);
        } catch (SecurityException e) {
          throw new IOException(e);
        } catch (NoSuchMethodException e) {
          throw new IOException(e);
        }
        S s;
        try {
          s = (S)m.invoke(null,request.getRowProcessorInitializerMessage());
        } catch (IllegalArgumentException e) {
          throw new IOException(e);
        } catch (InvocationTargetException e) {
          throw new IOException(e);
        }
        ci.initialize(s);
      }
      return ci;
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    } catch (InstantiationException e) {
      throw new IOException(e);
    } catch (IllegalAccessException e) {
      throw new IOException(e);
    }
  }
}
