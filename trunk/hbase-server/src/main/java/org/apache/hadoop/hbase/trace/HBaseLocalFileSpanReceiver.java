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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.cloudera.htrace.Span;
import org.cloudera.htrace.SpanReceiver;
import org.cloudera.htrace.Trace;
import org.cloudera.htrace.impl.LocalFileSpanReceiver;

/**
 * Wraps the LocalFileSpanReceiver provided in
 * org.cloudera.htrace.impl.LocalFileSpanReceiver to read the file name
 * destination for spans from hbase-site.xml.
 * 
 * The file path should be added as a property with name
 * "hbase.trace.spanreceiver.localfilespanreceiver.filename".
 */
public class HBaseLocalFileSpanReceiver implements SpanReceiver, Configurable {
  public static final Log LOG = LogFactory
      .getLog(HBaseLocalFileSpanReceiver.class);
  public static final String FILE_NAME_CONF_KEY = "hbase.trace.spanreceiver.localfilespanreceiver.filename";
  private Configuration conf;
  private LocalFileSpanReceiver rcvr;

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration arg0) {
    this.conf = arg0;
    // replace rcvr if it was already created
    if (rcvr != null) {
      try {
        rcvr.close();
      } catch (IOException e) {
        LOG.warn("Error closing LocalFileSpanReceiver.", e);
      }
    }
    try {
      rcvr = new LocalFileSpanReceiver(conf.get(FILE_NAME_CONF_KEY));
    } catch (IOException e) {
      Trace.removeReceiver(this);
      rcvr = null;
      LOG.warn(
          "Unable to initialize LocalFileSpanReceiver, removing owner (HBaseLocalFileSpanReceiver) from receiver list.",
          e);
    }
  }

  @Override
  public void close() throws IOException {
    try{
      if (rcvr != null) {
        rcvr.close();
      }
    } finally {
      rcvr = null;
    }
  }

  @Override
  public void receiveSpan(Span span) {
    if (rcvr != null) {
      rcvr.receiveSpan(span);
    }
  }
}
