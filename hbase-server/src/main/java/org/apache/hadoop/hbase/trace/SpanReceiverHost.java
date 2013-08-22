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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.cloudera.htrace.SpanReceiver;
import org.cloudera.htrace.Trace;

/**
 * This class provides functions for reading the names of SpanReceivers from
 * hbase-site.xml, adding those SpanReceivers to the Tracer, and closing those
 * SpanReceivers when appropriate.
 */
public class SpanReceiverHost {
  public static final String SPAN_RECEIVERS_CONF_KEY = "hbase.trace.spanreceiver.classes";
  private static final Log LOG = LogFactory.getLog(SpanReceiverHost.class);
  private Collection<SpanReceiver> receivers;
  private Configuration conf;
  private boolean closed = false;

  private static enum SingleTonholder {
    INSTANCE;
    Object lock = new Object();
    SpanReceiverHost host = null;
  }

  public static SpanReceiverHost getInstance(Configuration conf) {
    if (SingleTonholder.INSTANCE.host != null) {
      return SingleTonholder.INSTANCE.host;
    }
    synchronized (SingleTonholder.INSTANCE.lock) {
      if (SingleTonholder.INSTANCE.host != null) {
        return SingleTonholder.INSTANCE.host;
      }

      SpanReceiverHost host = new SpanReceiverHost(conf);
      host.loadSpanReceivers();
      SingleTonholder.INSTANCE.host = host;
      return SingleTonholder.INSTANCE.host;
    }

  }

  SpanReceiverHost(Configuration conf) {
    receivers = new HashSet<SpanReceiver>();
    this.conf = conf;
  }

  /**
   * Reads the names of classes specified in the
   * "hbase.trace.spanreceiver.classes" property and instantiates and registers
   * them with the Tracer as SpanReceiver's.
   *
   */
  public void loadSpanReceivers() {
    Class<?> implClass = null;
    String[] receiverNames = conf.getStrings(SPAN_RECEIVERS_CONF_KEY);
    if (receiverNames == null || receiverNames.length == 0) {
      return;
    }
    for (String className : receiverNames) {
      className = className.trim();

      try {
        implClass = Class.forName(className);
        SpanReceiver receiver = loadInstance(implClass);
        if (receiver != null) {
          receivers.add(receiver);
          LOG.info("SpanReceiver " + className + " was loaded successfully.");
        }

      } catch (ClassNotFoundException e) {
        LOG.warn("Class " + className + " cannot be found. " + e.getMessage());
      } catch (IOException e) {
        LOG.warn("Load SpanReceiver " + className + " failed. "
            + e.getMessage());
      }
    }
    for (SpanReceiver rcvr : receivers) {
      Trace.addReceiver(rcvr);
    }
  }

  private SpanReceiver loadInstance(Class<?> implClass)
      throws IOException {
    SpanReceiver impl = null;
    try {
      Object o = implClass.newInstance();
      impl = (SpanReceiver)o;
      impl.configure(new HBaseHTraceConfiguration(this.conf));
    } catch (SecurityException e) {
      throw new IOException(e);
    } catch (IllegalArgumentException e) {
      throw new IOException(e);
    } catch (RuntimeException e) {
      throw new IOException(e);
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }

    return impl;
  }

  /**
   * Calls close() on all SpanReceivers created by this SpanReceiverHost.
   */
  public synchronized void closeReceivers() {
    if (closed) return;
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
