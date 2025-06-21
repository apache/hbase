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
package org.apache.hadoop.hbase.namequeues;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.hbase.ExtendedCellScanner;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.ipc.RpcCall;
import org.apache.hadoop.hbase.ipc.RpcCallback;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.protobuf.BlockingService;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.apache.hbase.thirdparty.com.google.protobuf.CodedInputStream;
import org.apache.hbase.thirdparty.com.google.protobuf.Descriptors;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos;

@Category({ RegionServerTests.class, SmallTests.class })
public class TestRpcLogDetails {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRpcLogDetails.class);

  private final ClientProtos.Scan scan =
    ClientProtos.Scan.newBuilder().setStartRow(ByteString.copyFrom(Bytes.toBytes("abc")))
      .setStopRow(ByteString.copyFrom(Bytes.toBytes("xyz"))).build();
  private final ClientProtos.Scan otherScan =
    ClientProtos.Scan.newBuilder().setStartRow(ByteString.copyFrom(Bytes.toBytes("def")))
      .setStopRow(ByteString.copyFrom(Bytes.toBytes("uvw"))).build();
  private final ClientProtos.ScanRequest scanRequest = ClientProtos.ScanRequest
    .newBuilder(ClientProtos.ScanRequest.getDefaultInstance()).setScan(scan).build();
  private final ClientProtos.ScanRequest otherScanRequest = ClientProtos.ScanRequest
    .newBuilder(ClientProtos.ScanRequest.getDefaultInstance()).setScan(otherScan).build();

  @Test
  public void itDeepCopiesRpcLogDetailsParams() throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(scanRequest.toByteArray().length);
    CodedInputStream cis = UnsafeByteOperations.unsafeWrap(buffer).newCodedInput();
    cis.enableAliasing(true);
    buffer.put(scanRequest.toByteArray());
    Message.Builder messageBuilder = ClientProtos.ScanRequest.newBuilder();
    ProtobufUtil.mergeFrom(messageBuilder, cis, buffer.capacity());
    Message message = messageBuilder.build();
    RpcLogDetails rpcLogDetails =
      new RpcLogDetails(getRpcCall(message), message, null, 0L, 0L, 0, null, true, false);

    // log's scan should be equal
    ClientProtos.Scan logScan = ((ClientProtos.ScanRequest) rpcLogDetails.getParam()).getScan();
    assertEquals(logScan, scan);

    // ensure we have a different byte array for testing
    assertFalse(Arrays.equals(scanRequest.toByteArray(), otherScanRequest.toByteArray()));

    // corrupt the underlying buffer
    buffer.position(0);
    buffer.put(otherScanRequest.toByteArray(), 0, otherScanRequest.toByteArray().length);
    assertArrayEquals(otherScanRequest.toByteArray(), buffer.array());

    // log scan should still be original scan
    assertEquals(logScan, scan);
  }

  @SuppressWarnings("checkstyle:methodlength")
  private static RpcCall getRpcCall(Message message) {
    RpcCall rpcCall = new RpcCall() {
      @Override
      public BlockingService getService() {
        return null;
      }

      @Override
      public Descriptors.MethodDescriptor getMethod() {
        return null;
      }

      @Override
      public Message getParam() {
        return message;
      }

      @Override
      public ExtendedCellScanner getCellScanner() {
        return null;
      }

      @Override
      public long getReceiveTime() {
        return 0;
      }

      @Override
      public long getStartTime() {
        return 0;
      }

      @Override
      public void setStartTime(long startTime) {
      }

      @Override
      public int getTimeout() {
        return 0;
      }

      @Override
      public int getPriority() {
        return 0;
      }

      @Override
      public long getDeadline() {
        return 0;
      }

      @Override
      public long getSize() {
        return 0;
      }

      @Override
      public RPCProtos.RequestHeader getHeader() {
        return null;
      }

      @Override
      public Map<String, byte[]> getConnectionAttributes() {
        return Collections.emptyMap();
      }

      @Override
      public Map<String, byte[]> getRequestAttributes() {
        return Collections.emptyMap();
      }

      @Override
      public byte[] getRequestAttribute(String key) {
        return null;
      }

      @Override
      public int getRemotePort() {
        return 0;
      }

      @Override
      public void setResponse(Message param, ExtendedCellScanner cells, Throwable errorThrowable,
        String error) {
      }

      @Override
      public void sendResponseIfReady() throws IOException {
      }

      @Override
      public void cleanup() {
      }

      @Override
      public String toShortString() {
        return null;
      }

      @Override
      public long disconnectSince() {
        return 0;
      }

      @Override
      public boolean isClientCellBlockSupported() {
        return false;
      }

      @Override
      public Optional<User> getRequestUser() {
        return null;
      }

      @Override
      public Optional<X509Certificate[]> getClientCertificateChain() {
        return Optional.empty();
      }

      @Override
      public InetAddress getRemoteAddress() {
        return null;
      }

      @Override
      public HBaseProtos.VersionInfo getClientVersionInfo() {
        return null;
      }

      @Override
      public void setCallBack(RpcCallback callback) {
      }

      @Override
      public boolean isRetryImmediatelySupported() {
        return false;
      }

      @Override
      public long getResponseCellSize() {
        return 0;
      }

      @Override
      public void incrementResponseCellSize(long cellSize) {
      }

      @Override
      public long getBlockBytesScanned() {
        return 0;
      }

      @Override
      public void incrementBlockBytesScanned(long blockSize) {
      }

      @Override
      public long getResponseExceptionSize() {
        return 0;
      }

      @Override
      public void incrementResponseExceptionSize(long exceptionSize) {
      }

      @Override
      public CallType getCallType() {
        return CallType.NONE;
      }

      @Override
      public void setCallType(CallType type) {
      }

      @Override
      public void updateFsReadTime(long latencyMillis) {

      }

      @Override
      public long getFsReadTime() {
        return 0;
      }
    };
    return rpcCall;
  }

}
