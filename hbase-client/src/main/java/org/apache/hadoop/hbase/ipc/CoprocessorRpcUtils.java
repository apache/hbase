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
 *
 */

package org.apache.hadoop.hbase.ipc;

import static org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionSpecifier.RegionSpecifierType.REGION_NAME;
import java.io.IOException;
import java.io.InterruptedIOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.exceptions.UnknownProtocolException;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.UnsafeByteOperations;
import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CoprocessorServiceCall;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CoprocessorServiceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionSpecifier.RegionSpecifierType;
import org.apache.hadoop.util.StringUtils;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Descriptors.ServiceDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Service;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Utilities for handling coprocessor rpc service calls.
 */
@InterfaceAudience.Private
public final class CoprocessorRpcUtils {
  private static final Log LOG = LogFactory.getLog(CoprocessorRpcUtils.class);
  /**
   * We assume that all HBase protobuf services share a common package name
   * (defined in the .proto files).
   */
  private static final String hbaseServicePackage;
  static {
    Descriptors.ServiceDescriptor clientService = ClientProtos.ClientService.getDescriptor();
    hbaseServicePackage = clientService.getFullName()
        .substring(0, clientService.getFullName().lastIndexOf(clientService.getName()));
  }

  private CoprocessorRpcUtils() {
    // private for utility class
  }

  /**
   * Returns the name to use for coprocessor service calls.  For core HBase services
   * (in the hbase.pb protobuf package), this returns the unqualified name in order to provide
   * backward compatibility across the package name change.  For all other services,
   * the fully-qualified service name is used.
   */
  public static String getServiceName(Descriptors.ServiceDescriptor service) {
    if (service.getFullName().startsWith(hbaseServicePackage)) {
      return service.getName();
    }
    return service.getFullName();
  }

  public static CoprocessorServiceRequest getCoprocessorServiceRequest(
      final Descriptors.MethodDescriptor method, final Message request) {
    return getCoprocessorServiceRequest(method, request, HConstants.EMPTY_BYTE_ARRAY,
        HConstants.EMPTY_BYTE_ARRAY);
  }

  public static CoprocessorServiceRequest getCoprocessorServiceRequest(
      final Descriptors.MethodDescriptor method, final Message request, final byte [] row,
      final byte [] regionName) {
    return CoprocessorServiceRequest.newBuilder().setCall(
        getCoprocessorServiceCall(method, request, row)).
          setRegion(RequestConverter.buildRegionSpecifier(REGION_NAME, regionName)).build();
  }

  private static CoprocessorServiceCall getCoprocessorServiceCall(
      final Descriptors.MethodDescriptor method, final Message request, final byte [] row) {
    return CoprocessorServiceCall.newBuilder()
    .setRow(org.apache.hadoop.hbase.shaded.com.google.protobuf.UnsafeByteOperations.unsafeWrap(row))
    .setServiceName(CoprocessorRpcUtils.getServiceName(method.getService()))
    .setMethodName(method.getName())
    // TODO!!!!! Come back here after!!!!! This is a double copy of the request if I read
    // it right copying from non-shaded to shaded version!!!!!! FIXXXXX!!!!!
    .setRequest(org.apache.hadoop.hbase.shaded.com.google.protobuf.UnsafeByteOperations.
        unsafeWrap(request.toByteArray())).build();
  }

  public static MethodDescriptor getMethodDescriptor(final String methodName,
      final ServiceDescriptor serviceDesc)
  throws UnknownProtocolException {
    Descriptors.MethodDescriptor methodDesc = serviceDesc.findMethodByName(methodName);
    if (methodDesc == null) {
      throw new UnknownProtocolException("Unknown method " + methodName + " called on service " +
          serviceDesc.getFullName());
    }
    return methodDesc;
  }

  public static Message getRequest(Service service,
      Descriptors.MethodDescriptor methodDesc,
      org.apache.hadoop.hbase.shaded.com.google.protobuf.ByteString shadedRequest)
  throws IOException {
    Message.Builder builderForType =
        service.getRequestPrototype(methodDesc).newBuilderForType();
    org.apache.hadoop.hbase.protobuf.ProtobufUtil.mergeFrom(builderForType,
        // TODO: COPY FROM SHADED TO NON_SHADED. DO I HAVE TOO?
        shadedRequest.toByteArray());
    return builderForType.build();
  }

  public static Message getResponse(
      org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CoprocessorServiceResponse
        result,
      com.google.protobuf.Message responsePrototype)
  throws IOException {
    Message response;
    if (result.getValue().hasValue()) {
      Message.Builder builder = responsePrototype.newBuilderForType();
      builder.mergeFrom(result.getValue().getValue().newInput());
      response = builder.build();
    } else {
      response = responsePrototype.getDefaultInstanceForType();
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("Master Result is value=" + response);
    }
    return response;
  }

  public static org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.
      CoprocessorServiceResponse getResponse(final Message result, final byte [] regionName) {
    org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.
      CoprocessorServiceResponse.Builder builder =
        org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CoprocessorServiceResponse.
        newBuilder();
    builder.setRegion(RequestConverter.buildRegionSpecifier(RegionSpecifierType.REGION_NAME,
      regionName));
    // TODO: UGLY COPY IN HERE!!!!
    builder.setValue(builder.getValueBuilder().setName(result.getClass().getName())
        .setValue(org.apache.hadoop.hbase.shaded.com.google.protobuf.ByteString.
            copyFrom(result.toByteArray())));
    return builder.build();
  }

  /**
   * Simple {@link RpcCallback} implementation providing a
   * {@link java.util.concurrent.Future}-like {@link BlockingRpcCallback#get()} method, which
   * will block util the instance's {@link BlockingRpcCallback#run(Object)} method has been called.
   * {@code R} is the RPC response type that will be passed to the {@link #run(Object)} method.
   */
  @InterfaceAudience.Private
  // Copy of BlockingRpcCallback but deriving from RpcCallback non-shaded.
  public static class BlockingRpcCallback<R> implements RpcCallback<R> {
    private R result;
    private boolean resultSet = false;

    /**
     * Called on completion of the RPC call with the response object, or {@code null} in the case of
     * an error.
     * @param parameter the response object or {@code null} if an error occurred
     */
    @Override
    public void run(R parameter) {
      synchronized (this) {
        result = parameter;
        resultSet = true;
        this.notifyAll();
      }
    }

    /**
     * Returns the parameter passed to {@link #run(Object)} or {@code null} if a null value was
     * passed.  When used asynchronously, this method will block until the {@link #run(Object)}
     * method has been called.
     * @return the response object or {@code null} if no response was passed
     */
    public synchronized R get() throws IOException {
      while (!resultSet) {
        try {
          this.wait();
        } catch (InterruptedException ie) {
          InterruptedIOException exception = new InterruptedIOException(ie.getMessage());
          exception.initCause(ie);
          throw exception;
        }
      }
      return result;
    }
  }

  /**
   * Stores an exception encountered during RPC invocation so it can be passed back
   * through to the client.
   * @param controller the controller instance provided by the client when calling the service
   * @param ioe the exception encountered
   */
  public static void setControllerException(RpcController controller, IOException ioe) {
    if (controller == null) {
      return;
    }
    if (controller instanceof org.apache.hadoop.hbase.ipc.ServerRpcController) {
      ((ServerRpcController)controller).setFailedOn(ioe);
    } else {
      controller.setFailed(StringUtils.stringifyException(ioe));
    }
  }

  /**
   * Retreivies exception stored during RPC invocation.
   * @param controller the controller instance provided by the client when calling the service
   * @return exception if any, or null; Will return DoNotRetryIOException for string represented
   * failure causes in controller.
   */
  @Nullable
  public static IOException getControllerException(RpcController controller) throws IOException {
    if (controller == null || !controller.failed()) {
      return null;
    }
    if (controller instanceof ServerRpcController) {
      return ((ServerRpcController)controller).getFailedOn();
    }
    return new DoNotRetryIOException(controller.errorText());
  }
}