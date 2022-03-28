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

package org.apache.hadoop.hbase.server.trace;

import static org.apache.hadoop.hbase.trace.HBaseSemanticAttributes.RPC_METHOD;
import static org.apache.hadoop.hbase.trace.HBaseSemanticAttributes.RPC_SERVICE;
import static org.apache.hadoop.hbase.trace.HBaseSemanticAttributes.RPC_SYSTEM;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.hadoop.hbase.client.trace.IpcClientSpanBuilder;
import org.apache.hadoop.hbase.ipc.RpcCall;
import org.apache.hadoop.hbase.trace.HBaseSemanticAttributes.RpcSystem;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hbase.thirdparty.com.google.protobuf.BlockingService;

/**
 * Construct {@link Span} instances originating from the server side of an IPC.
 *
 * @see <a href="https://github.com/open-telemetry/opentelemetry-specification/blob/3e380e249f60c3a5f68746f5e84d10195ba41a79/specification/trace/semantic_conventions/rpc.md">Semantic conventions for RPC spans</a>
 */
@InterfaceAudience.Private
public class IpcServerSpanBuilder implements Supplier<Span> {

  private String name;
  private final Map<AttributeKey<?>, Object> attributes = new HashMap<>();

  public IpcServerSpanBuilder(final RpcCall rpcCall) {
    final String packageAndService = Optional.ofNullable(rpcCall.getService())
      .map(BlockingService::getDescriptorForType)
      .map(IpcClientSpanBuilder::getRpcPackageAndService)
      .orElse("");
    final String method = Optional.ofNullable(rpcCall.getMethod())
      .map(IpcClientSpanBuilder::getRpcName)
      .orElse("");
    setName(IpcClientSpanBuilder.buildSpanName(packageAndService, method));
    addAttribute(RPC_SYSTEM, RpcSystem.HBASE_RPC.name());
    addAttribute(RPC_SERVICE, packageAndService);
    addAttribute(RPC_METHOD, method);
  }

  @Override
  public Span get() {
    return build();
  }

  public IpcServerSpanBuilder setName(final String name) {
    this.name = name;
    return this;
  }

  public <T> IpcServerSpanBuilder addAttribute(final AttributeKey<T> key, T value) {
    attributes.put(key, value);
    return this;
  }

  @SuppressWarnings("unchecked")
  public Span build() {
    final SpanBuilder builder = TraceUtil.getGlobalTracer()
      .spanBuilder(name)
      .setSpanKind(SpanKind.SERVER);
    attributes.forEach((k, v) -> builder.setAttribute((AttributeKey<? super Object>) k, v));
    return builder.startSpan();
  }
}
