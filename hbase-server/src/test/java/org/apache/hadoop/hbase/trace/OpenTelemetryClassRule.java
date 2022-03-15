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
package org.apache.hadoop.hbase.trace;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.testing.junit4.OpenTelemetryRule;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import java.util.List;
import org.junit.rules.ExternalResource;

/**
 * <p>Like {@link OpenTelemetryRule}, except modeled after the junit5 implementation
 * {@code OpenTelemetryExtension}. Use this class when you need to make asserts on {@link SpanData}
 * created on a MiniCluster. Make sure this rule initialized before the MiniCluster so that it can
 * register its instance of {@link OpenTelemetry} as the global instance before any server-side
 * component can call {@link TraceUtil#getGlobalTracer()}.</p>
 * <p>For example:</p>
 * <pre>{@code
 * public class TestMyClass {
 *   private static final OpenTelemetryClassRule otelClassRule =
 *     OpenTelemetryClassRule.create();
 *   private static final MiniClusterRule miniClusterRule =
 *     MiniClusterRule.newBuilder().build();
 *   protected static final ConnectionRule connectionRule =
 *     new ConnectionRule(miniClusterRule::createConnection);
 *
 *   @ClassRule
 *   public static final TestRule classRule = RuleChain.outerRule(otelClassRule)
 *     .around(miniClusterRule)
 *     .around(connectionRule);
 *
 *   @Rule
 *   public final OpenTelemetryTestRule otelTestRule =
 *     new OpenTelemetryTestRule(otelClassRule);
 *
 *   @Test
 *   public void myTest() {
 *     // ...
 *     // do something that makes spans
 *     final List<SpanData> spans = otelClassRule.getSpans();
 *     // make assertions on them
 *   }
 * }
 * }</pre>
 *
 * @see <a href="https://github.com/open-telemetry/opentelemetry-java/blob/9a330d0/sdk/testing/src/main/java/io/opentelemetry/sdk/testing/junit5/OpenTelemetryExtension.java">junit5/OpenTelemetryExtension.java</a>
 */
public final class OpenTelemetryClassRule extends ExternalResource {

  public static OpenTelemetryClassRule create() {
    InMemorySpanExporter spanExporter = InMemorySpanExporter.create();

    SdkTracerProvider tracerProvider =
      SdkTracerProvider.builder()
        .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
        .build();

    OpenTelemetrySdk openTelemetry =
      OpenTelemetrySdk.builder()
        .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
        .setTracerProvider(tracerProvider)
        .build();

    return new OpenTelemetryClassRule(openTelemetry, spanExporter);
  }

  private final OpenTelemetrySdk openTelemetry;
  private final InMemorySpanExporter spanExporter;

  private OpenTelemetryClassRule(
    final OpenTelemetrySdk openTelemetry,
    final InMemorySpanExporter spanExporter
  ) {
    this.openTelemetry = openTelemetry;
    this.spanExporter = spanExporter;
  }

  /** Returns the {@link OpenTelemetry} created by this Rule. */
  public OpenTelemetry getOpenTelemetry() {
    return openTelemetry;
  }

  /** Returns all the exported {@link SpanData} so far. */
  public List<SpanData> getSpans() {
    return spanExporter.getFinishedSpanItems();
  }

  /**
   * Clears the collected exported {@link SpanData}.
   */
  public void clearSpans() {
    spanExporter.reset();
  }

  @Override
  protected void before() throws Throwable {
    GlobalOpenTelemetry.resetForTest();
    GlobalOpenTelemetry.set(openTelemetry);
  }

  @Override
  protected void after() {
    GlobalOpenTelemetry.resetForTest();
  }
}
