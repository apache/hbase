/**
 *
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

import java.util.HashMap;

import org.apache.hadoop.hbase.classification.InterfaceAudience;

@InterfaceAudience.Private
public class MetricsHBaseServerSourceFactoryImpl extends MetricsHBaseServerSourceFactory {
  private enum SourceStorage {
    INSTANCE;
    HashMap<String, MetricsHBaseServerSource>
        sources =
        new HashMap<String, MetricsHBaseServerSource>();

  }

  @Override
  public MetricsHBaseServerSource create(String serverName, MetricsHBaseServerWrapper wrapper) {
    return getSource(serverName, wrapper);
  }

  private static synchronized MetricsHBaseServerSource getSource(String serverName,
                                                                 MetricsHBaseServerWrapper wrapper) {
    String context = createContextName(serverName);
    MetricsHBaseServerSource source = SourceStorage.INSTANCE.sources.get(context);

    if (source == null) {
      //Create the source.
      source = new MetricsHBaseServerSourceImpl(
          METRICS_NAME,
          METRICS_DESCRIPTION,
          context.toLowerCase(),
          context + METRICS_JMX_CONTEXT_SUFFIX, wrapper);

      //Store back in storage
      SourceStorage.INSTANCE.sources.put(context, source);
    }

    return source;

  }

}
