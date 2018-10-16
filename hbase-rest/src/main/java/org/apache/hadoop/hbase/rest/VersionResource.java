/*
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

package org.apache.hadoop.hbase.rest;

import java.io.IOException;

import javax.servlet.ServletContext;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.CacheControl;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.core.Response.ResponseBuilder;

import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.rest.model.VersionModel;

/**
 * Implements REST software version reporting
 * <p>
 * <tt>/version/rest</tt>
 * <p>
 * <tt>/version</tt> (alias for <tt>/version/rest</tt>)
 */
@InterfaceAudience.Private
public class VersionResource extends ResourceBase {

  private static final Logger LOG = LoggerFactory.getLogger(VersionResource.class);

  static CacheControl cacheControl;
  static {
    cacheControl = new CacheControl();
    cacheControl.setNoCache(true);
    cacheControl.setNoTransform(false);
  }

  /**
   * Constructor
   * @throws IOException
   */
  public VersionResource() throws IOException {
    super();
  }

  /**
   * Build a response for a version request.
   * @param context servlet context
   * @param uriInfo (JAX-RS context variable) request URL
   * @return a response for a version request
   */
  @GET
  @Produces({MIMETYPE_TEXT, MIMETYPE_XML, MIMETYPE_JSON, MIMETYPE_PROTOBUF,
    MIMETYPE_PROTOBUF_IETF})
  public Response get(final @Context ServletContext context,
      final @Context UriInfo uriInfo) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("GET " + uriInfo.getAbsolutePath());
    }
    servlet.getMetrics().incrementRequests(1);
    ResponseBuilder response = Response.ok(new VersionModel(context));
    response.cacheControl(cacheControl);
    servlet.getMetrics().incrementSucessfulGetRequests(1);
    return response.build();
  }

  /**
   * Dispatch to StorageClusterVersionResource
   */
  @Path("cluster")
  public StorageClusterVersionResource getClusterVersionResource()
      throws IOException {
    return new StorageClusterVersionResource();
  }

  /**
   * Dispatch <tt>/version/rest</tt> to self.
   */
  @Path("rest")
  public VersionResource getVersionResource() {
    return this;
  }
}
