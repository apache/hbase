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

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.htrace.core.SpanReceiver;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides functions for reading the names of SpanReceivers from
 * hbase-site.xml, adding those SpanReceivers to the Tracer, and closing those
 * SpanReceivers when appropriate.
 */
@InterfaceAudience.Private
public class SpanReceiverHost {
  public static final String SPAN_RECEIVERS_CONF_KEY = "hbase.trace.spanreceiver.classes";
  private static final Logger LOG = LoggerFactory.getLogger(SpanReceiverHost.class);
  private Collection<SpanReceiver> receivers;
  private Configuration conf;
  private boolean closed = false;

  private enum SingletonHolder {
    INSTANCE;
    final transient Object lock = new Object();
    transient SpanReceiverHost host = null;
  }

  public static SpanReceiverHost getInstance(Configuration conf) {
    synchronized (SingletonHolder.INSTANCE.lock) {
      if (SingletonHolder.INSTANCE.host != null) {
        return SingletonHolder.INSTANCE.host;
      }

      SpanReceiverHost host = new SpanReceiverHost(conf);
      host.loadSpanReceivers();
      SingletonHolder.INSTANCE.host = host;
      return SingletonHolder.INSTANCE.host;
    }

  }

  public static Configuration getConfiguration(){
    synchronized (SingletonHolder.INSTANCE.lock) {
      if (SingletonHolder.INSTANCE.host == null || SingletonHolder.INSTANCE.host.conf == null) {
        return null;
      }

      return SingletonHolder.INSTANCE.host.conf;
    }
  }

  SpanReceiverHost(Configuration conf) {
    receivers = new HashSet<>();
    this.conf = conf;
  }

  /**
   * Reads the names of classes specified in the {@code hbase.trace.spanreceiver.classes} property
   * and instantiates and registers them with the Tracer.
   */
  public void loadSpanReceivers() {
    String[] receiverNames = conf.getStrings(SPAN_RECEIVERS_CONF_KEY);
    if (receiverNames == null || receiverNames.length == 0) {
      return;
    }

    SpanReceiver.Builder builder = new SpanReceiver.Builder(new HBaseHTraceConfiguration(conf));
    for (String className : receiverNames) {
      className = className.trim();

      SpanReceiver receiver = builder.className(className).build();
      if (receiver != null) {
        receivers.add(receiver);
        LOG.info("SpanReceiver {} was loaded successfully.", className);
      }
    }
    for (SpanReceiver rcvr : receivers) {
      TraceUtil.addReceiver(rcvr);
    }
  }

  /**
   * Calls close() on all SpanReceivers created by this SpanReceiverHost.
   */
  public synchronized void closeReceivers() {
    if (closed) {
      return;
    }

    closed = true;
    for (SpanReceiver rcvr : receivers) {
      try {
        rcvr.close();
      } catch (IOException e) {
        LOG.warn("Unable to close SpanReceiver correctly: " + e.getMessage(), e);
      }
    }
  }
}
