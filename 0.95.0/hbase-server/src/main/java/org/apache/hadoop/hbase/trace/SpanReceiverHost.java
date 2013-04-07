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

  public SpanReceiverHost(Configuration conf) {
    receivers = new HashSet<SpanReceiver>();
    this.conf = conf;
  }

  /**
   * Reads the names of classes specified in the
   * "hbase.trace.spanreceiver.classes" property and instantiates and registers
   * them with the Tracer as SpanReceiver's.
   * 
   * The nullary constructor is called during construction, but if the classes
   * specified implement the Configurable interface, setConfiguration() will be
   * called on them. This allows SpanReceivers to use values from
   * hbase-site.xml. See
   * {@link org.apache.hadoop.hbase.trace.HBaseLocalFileSpanReceiver} for an
   * example.
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
        receivers.add(loadInstance(implClass));
        LOG.info("SpanReceiver " + className + " was loaded successfully.");
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
    SpanReceiver impl;
    try {
      Object o = ReflectionUtils.newInstance(implClass, conf);
      impl = (SpanReceiver)o;
    } catch (SecurityException e) {
      throw new IOException(e);
    } catch (IllegalArgumentException e) {
      throw new IOException(e);
    } catch (RuntimeException e) {
      throw new IOException(e);
    }

    return impl;
  }

  /**
   * Calls close() on all SpanReceivers created by this SpanReceiverHost.
   */
  public void closeReceivers() {
    for (SpanReceiver rcvr : receivers) {
      try {
        rcvr.close();
      } catch (IOException e) {
        LOG.warn("Unable to close SpanReceiver correctly: " + e.getMessage(), e);
      }
    }
  }
}
