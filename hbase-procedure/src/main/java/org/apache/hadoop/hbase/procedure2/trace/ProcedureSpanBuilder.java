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
package org.apache.hadoop.hbase.procedure2.trace;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import java.util.function.Supplier;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Construct a {@link Span} instance for a {@link Procedure} execution.
 */
@InterfaceAudience.Private
public class ProcedureSpanBuilder implements Supplier<Span> {

  private final String name;
  private final long procId;
  private final long parentProcId;

  public ProcedureSpanBuilder(final Procedure<?> proc) {
    name = proc.getProcName();
    procId = proc.getProcId();
    parentProcId = proc.getParentProcId();
  }

  @Override
  public Span get() {
    return build();
  }

  public Span build() {
    return TraceUtil.getGlobalTracer().spanBuilder(name).setSpanKind(SpanKind.INTERNAL)
      .setAttribute("procId", procId).setAttribute("parentProcId", parentProcId).startSpan();
  }
}
