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
package org.apache.hadoop.hbase.master.http.api_v1.cluster_metrics.resource;

import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.ExecutionException;
import javax.inject.Inject;
import org.apache.hadoop.hbase.ClusterMetrics.Option;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.http.api_v1.cluster_metrics.model.ClusterMetrics;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hbase.thirdparty.javax.ws.rs.GET;
import org.apache.hbase.thirdparty.javax.ws.rs.Path;
import org.apache.hbase.thirdparty.javax.ws.rs.Produces;
import org.apache.hbase.thirdparty.javax.ws.rs.core.MediaType;

/**
 * The root object exposing a subset of {@link org.apache.hadoop.hbase.ClusterMetrics}.
 */
@InterfaceAudience.Private
@Path("admin/cluster_metrics")
@Produces({ MediaType.APPLICATION_JSON })
public class ClusterMetricsResource {

  // TODO: using the async client API lends itself well to using the JAX-RS 2.0 Spec's asynchronous
  //  server APIs. However, these are only available when Jersey is wired up using Servlet 3.x
  //  container and all of our existing InfoServer stuff is build on Servlet 2.x.
  //  See also https://blog.allegro.tech/2014/10/async-rest.html#mixing-with-completablefuture

  private final AsyncAdmin admin;

  @Inject
  public ClusterMetricsResource(MasterServices master) {
    this.admin = master.getAsyncConnection().getAdmin();
  }

  private org.apache.hadoop.hbase.ClusterMetrics get(EnumSet<Option> fields)
    throws ExecutionException, InterruptedException {
    return admin.getClusterMetrics(fields).get();
  }

  @GET
  @Path("/")
  public ClusterMetrics getBaseMetrics() throws ExecutionException, InterruptedException {
    final EnumSet<Option> fields = EnumSet.of(
      Option.HBASE_VERSION,
      Option.CLUSTER_ID,
      Option.MASTER,
      Option.BACKUP_MASTERS
    );
    return ClusterMetrics.from(get(fields));
  }

  @GET
  @Path("/live_servers")
  public Collection<ServerMetrics> getLiveServers() throws ExecutionException,
    InterruptedException {
    return get(EnumSet.of(Option.LIVE_SERVERS)).getLiveServerMetrics().values();
  }

  @GET
  @Path("/dead_servers")
  public List<ServerName> getDeadServers() throws ExecutionException,
    InterruptedException {
    return get(EnumSet.of(Option.DEAD_SERVERS)).getDeadServerNames();
  }
}
