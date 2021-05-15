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
package org.apache.hadoop.hbase.master.http.api_v1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.http.jersey.ResponseEntityMapper;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.http.gson.GsonSerializationFeature;
import org.apache.hadoop.hbase.master.http.jersey.MasterFeature;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hbase.thirdparty.org.glassfish.jersey.server.ResourceConfig;
import org.apache.hbase.thirdparty.org.glassfish.jersey.server.ServerProperties;
import org.apache.hbase.thirdparty.org.glassfish.jersey.server.TracingConfig;

/**
 * Encapsulates construction and configuration of the {@link ResourceConfig} that implements
 * the {@code cluster-metrics} endpoints.
 */
@InterfaceAudience.Private
public final class ResourceConfigFactory {

  private ResourceConfigFactory() {}

  public static ResourceConfig createResourceConfig(Configuration conf, HMaster master) {
    return new ResourceConfig()
      .setApplicationName("api_v1")
      .packages(ResourceConfigFactory.class.getPackage().getName())
      // TODO: anything registered here that does not have necessary bindings won't inject properly
      //   at annotation sites and will result in a WARN logged by o.a.h.t.o.g.j.i.inject.Providers.
      //   These warnings should be treated by the service as fatal errors, but I have not found a
      //   callback API for registering a failed binding handler.
      .register(ResponseEntityMapper.class)
      .register(GsonSerializationFeature.class)
      .register(new MasterFeature(master))

      // devs: enable TRACING to see how jersey is dispatching to resources.
      // in hbase-site.xml, set 'hbase.http.jersey.tracing.type=ON_DEMAND` and
      // to curl, add `-H X-Jersey-Tracing-Accept:true`
      .property(ServerProperties.TRACING, conf.get(
        "hbase.http.jersey.tracing.type", TracingConfig.OFF.name()))
      .property(ServerProperties.TRACING_THRESHOLD, conf.get(
        "hbase.http.jersey.tracing.threshold", "TRACE"));
  }
}
