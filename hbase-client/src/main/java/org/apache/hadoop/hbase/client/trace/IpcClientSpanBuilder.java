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

package org.apache.hadoop.hbase.client.trace;

import static org.apache.hadoop.hbase.trace.HBaseSemanticAttributes.NET_PEER_NAME;
import static org.apache.hadoop.hbase.trace.HBaseSemanticAttributes.NET_PEER_PORT;
import static org.apache.hadoop.hbase.trace.HBaseSemanticAttributes.RPC_METHOD;
import static org.apache.hadoop.hbase.trace.HBaseSemanticAttributes.RPC_SERVICE;
import static org.apache.hadoop.hbase.trace.HBaseSemanticAttributes.RPC_SYSTEM;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.trace.HBaseSemanticAttributes.RpcSystem;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hbase.thirdparty.com.google.protobuf.Descriptors;

/**
 * Construct {@link Span} instances originating from the client side of an IPC.
 *
 * @see <a href="https://github.com/open-telemetry/opentelemetry-specification/blob/3e380e249f60c3a5f68746f5e84d10195ba41a79/specification/trace/semantic_conventions/rpc.md">Semantic conventions for RPC spans</a>
 */
@InterfaceAudience.Private
public class IpcClientSpanBuilder implements Supplier<Span> {

  private String name;
  private final Map<AttributeKey<?>, Object> attributes = new HashMap<>();

  @Override
  public Span get() {
    return build();
  }

  public IpcClientSpanBuilder setMethodDescriptor(final Descriptors.MethodDescriptor md) {
    final String packageAndService = getRpcPackageAndService(md.getService());
    final String method = getRpcName(md);
    this.name = buildSpanName(packageAndService, method);
    populateMethodDescriptorAttributes(attributes, md);
    return this;
  }

  public IpcClientSpanBuilder setRemoteAddress(final Address remoteAddress) {
    attributes.put(NET_PEER_NAME, remoteAddress.getHostName());
    attributes.put(NET_PEER_PORT, (long) remoteAddress.getPort());
    return this;
  }

  @SuppressWarnings("unchecked")
  public Span build() {
    final SpanBuilder builder = TraceUtil.getGlobalTracer()
      .spanBuilder(name)
      // TODO: what about clients embedded in Master/RegionServer/Gateways/&c?
      .setSpanKind(SpanKind.CLIENT);
    attributes.forEach((k, v) -> builder.setAttribute((AttributeKey<? super Object>) k, v));
    return builder.startSpan();
  }

  /**
   * Static utility method that performs the primary logic of this builder. It is visible to other
   * classes in this package so that other builders can use this functionality as a mix-in.
   * @param attributes the attributes map to be populated.
   * @param md the source of the RPC attribute values.
   */
  static void populateMethodDescriptorAttributes(
    final Map<AttributeKey<?>, Object> attributes,
    final Descriptors.MethodDescriptor md
  ) {
    final String packageAndService = getRpcPackageAndService(md.getService());
    final String method = getRpcName(md);
    attributes.put(RPC_SYSTEM, RpcSystem.HBASE_RPC.name());
    attributes.put(RPC_SERVICE, packageAndService);
    attributes.put(RPC_METHOD, method);
  }

  /**
   * Retrieve the combined {@code $package.$service} value from {@code sd}.
   */
  public static String getRpcPackageAndService(final Descriptors.ServiceDescriptor sd) {
    // it happens that `getFullName` returns a string in the $package.$service format required by
    // the otel RPC specification. Use it for now; might have to parse the value in the future.
    return sd.getFullName();
  }

  /**
   * Retrieve the {@code $method} value from {@code md}.
   */
  public static String getRpcName(final Descriptors.MethodDescriptor md) {
    return md.getName();
  }

  /**
   * Construct an RPC span name.
   */
  public static String buildSpanName(final String packageAndService, final String method) {
    return packageAndService + "/" + method;
  }
}
