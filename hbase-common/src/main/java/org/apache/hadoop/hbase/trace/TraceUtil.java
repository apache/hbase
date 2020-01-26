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
package org.apache.hadoop.hbase.trace;

import io.opentracing.Scope;
import io.opentracing.SpanContext;
import io.opentracing.mock.MockTracer;
import io.opentracing.propagation.Format;
import io.opentracing.propagation.TextMapExtractAdapter;
import io.opentracing.propagation.TextMapInjectAdapter;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import io.opentracing.propagation.TextMapExtractAdapter;
import io.opentracing.propagation.TextMapInjectAdapter;
import io.opentracing.util.GlobalTracer;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.Configuration;
import org.apache.htrace.core.HTraceConfiguration;
import org.apache.htrace.core.Sampler;
import org.apache.htrace.core.SpanReceiver;
import org.apache.htrace.core.TraceScope;
import io.jaegertracing.Configuration.SamplerConfiguration;

import org.apache.hadoop.tracing.TraceUtils;
import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * This wrapper class provides functions for accessing htrace 4+ functionality in a simplified way.
 */
@InterfaceAudience.Private
public final class TraceUtil {
  static final Logger LOG = LoggerFactory.getLogger(TraceUtils.class);

  private static io.jaegertracing.Configuration conf;
  private static io.opentracing.Tracer tracer;

  public static final String HBASE_OPENTRACING_TRACER = "hbase.opentracing.tracer";
  public static final String HBASE_OPENTRACING_TRACER_DEFAULT = "jaeger";
  public static final String HBASE_OPENTRACING_MOCKTRACER = "mock";

  private TraceUtil() {
  }

  public static void initTracer(Configuration c, String serviceName) {
    if (GlobalTracer.isRegistered()) {
      LOG.info("A tracer is already registered.");
      return;
    }

    switch(c.get(HBASE_OPENTRACING_TRACER, HBASE_OPENTRACING_TRACER_DEFAULT)) {
    case HBASE_OPENTRACING_TRACER_DEFAULT:
      io.jaegertracing.Configuration conf = io.jaegertracing.Configuration.fromEnv(serviceName);
      tracer = conf.getTracerBuilder().build();
      break;
    case HBASE_OPENTRACING_MOCKTRACER:
      tracer = new MockTracer();
      break;
    default:
      throw new RuntimeException("Unexpected tracer");
    }


    GlobalTracer.register(tracer);

  }

  /*@VisibleForTesting
  public static void registerTracerForTest(Tracer tracer) {
    TraceUtil.tracer = tracer;
    GlobalTracer.register(tracer);
  }*/

  public static Tracer getTracer() {
    return tracer;
  }

  /**
   * Wrapper method to create new Scope with the given description
   * @return Scope or null when not tracing
   */
  public static Scope createTrace(String description) {
    return (tracer == null) ? null :
        tracer.buildSpan(description).startActive(true);
  }

  /**
   * Wrapper method to create new child Scope with the given description
   * and parent scope's spanId
   * @param span parent span
   * @return Scope or null when not tracing
   */
  public static Scope createTrace(String description, Span span) {
    if(span == null) return createTrace(description);

    return (tracer == null) ? null : tracer.buildSpan(description).
        asChildOf(span).startActive(true);
  }

  public static Scope createTrace(String description, SpanContext spanContext) {
    if(spanContext == null) return createTrace(description);

    return (tracer == null) ? null : tracer.buildSpan(description).
        asChildOf(spanContext).startActive(true);
  }

  /**
   * Wrapper method to add new sampler to the default tracer
   * @return true if added, false if it was already added
   */
  public static boolean addSampler(SamplerConfiguration sampler) {
    if (sampler == null) {
      return false;
    }

    conf = conf.withSampler(sampler);
    tracer = conf.getTracerBuilder().build();

    GlobalTracer.register(tracer);
    return true;
  }

  /**
   * Wrapper method to add key-value pair to TraceInfo of actual span
   */
  public static void addKVAnnotation(String key, String value){
    Span span = tracer.activeSpan();
    if (span != null) {
      span.setTag(key, value);
    }
  }

  /**
   * Wrapper method to add receiver to actual tracerpool
   * @return true if successfull, false if it was already added
   */
  /*public static boolean addReceiver(SpanReceiver rcvr) {
    return (tracer == null) ? false : tracer.getTracerPool().addReceiver(rcvr);
  }*/

  /**
   * Wrapper method to remove receiver from actual tracerpool
   * @return true if removed, false if doesn't exist
   */
  /*public static boolean removeReceiver(SpanReceiver rcvr) {
    return (tracer == null) ? false : tracer.getTracerPool().removeReceiver(rcvr);
  }*/

  /**
   * Wrapper method to add timeline annotiation to current span with given message
   */
  public static void addTimelineAnnotation(String msg) {
    Span span = tracer.activeSpan();
    if (span != null) {
      span.log(msg);
    }
  }

  /**
   * Wrap runnable with current tracer and description
   * @param runnable to wrap
   * @return wrapped runnable or original runnable when not tracing
   */
  public static Runnable wrap(Runnable runnable, String description) {
    //return (tracer == null) ? runnable : tracer.wrap(runnable, description);
    return runnable;
  }

  public static SpanContext byteArrayToSpanContext(byte[] byteArray) {
    if (byteArray == null || byteArray.length == 0) {
      LOG.debug("The provided serialized context was null or empty");
      return null;
    }

    SpanContext context = null;
    ByteArrayInputStream stream = new ByteArrayInputStream(byteArray);

    try {
      ObjectInputStream objStream = new ObjectInputStream(stream);
      Map<String, String> carrier = (Map<String, String>) objStream.readObject();

      context = GlobalTracer.get().extract(Format.Builtin.TEXT_MAP,
        new TextMapExtractAdapter(carrier));
    } catch (Exception e) {
      LOG.warn("Could not deserialize context {}", Hex.encodeHexString(byteArray), e);
    }

    return context;
  }

  public static byte[] spanContextToByteArray(SpanContext context) {
    if (context == null) {
      LOG.debug("No SpanContext was provided");
      return null;
    }

    Map<String, String> carrier = new HashMap<String, String>();
    GlobalTracer.get().inject(context, Format.Builtin.TEXT_MAP,
      new TextMapInjectAdapter(carrier));
    if (carrier.isEmpty()) {
      LOG.warn("SpanContext was not properly injected by the Tracer.");
      return null;
    }

    byte[] byteArray = null;
    ByteArrayOutputStream stream = new ByteArrayOutputStream();

    try {
      ObjectOutputStream objStream = new ObjectOutputStream(stream);
      objStream.writeObject(carrier);
      objStream.flush();

      byteArray = stream.toByteArray();
      LOG.debug("SpanContext serialized, resulting byte length is {}",
        byteArray.length);
    } catch (IOException e) {
      LOG.warn("Could not serialize context {}", context, e);
    }

    return byteArray;
  }
}
