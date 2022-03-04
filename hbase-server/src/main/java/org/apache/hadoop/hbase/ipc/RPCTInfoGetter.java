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

import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.shaded.protobuf.generated.TracingProtos;

/**
 * Used to extract a tracing {@link Context} from an instance of {@link TracingProtos.RPCTInfo}.
 */
@InterfaceAudience.Private
final class RPCTInfoGetter implements TextMapGetter<TracingProtos.RPCTInfo> {
  RPCTInfoGetter() { }

  @Override
  public Iterable<String> keys(TracingProtos.RPCTInfo carrier) {
    return Optional.ofNullable(carrier)
      .map(TracingProtos.RPCTInfo::getHeadersMap)
      .map(Map::keySet)
      .orElse(Collections.emptySet());
  }

  @Override
  public String get(TracingProtos.RPCTInfo carrier, String key) {
    return Optional.ofNullable(carrier)
      .map(TracingProtos.RPCTInfo::getHeadersMap)
      .map(map -> map.get(key))
      .orElse(null);
  }
}
