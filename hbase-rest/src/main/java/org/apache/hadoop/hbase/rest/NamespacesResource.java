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
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.rest.model.NamespacesModel;

/**
 * Implements REST GET list of all namespaces.
 * <p>
 * <tt>/namespaces</tt>
 * <p>
 */
@InterfaceAudience.Private
public class NamespacesResource extends ResourceBase {

  private static final Log LOG = LogFactory.getLog(NamespacesResource.class);

  /**
   * Constructor
   * @throws IOException
   */
  public NamespacesResource() throws IOException {
    super();
  }

  /**
   * Build a response for a list of all namespaces request.
   * @param context servlet context
   * @param uriInfo (JAX-RS context variable) request URL
   * @return a response for a version request
   */
  @GET
  @Produces({MIMETYPE_TEXT, MIMETYPE_XML, MIMETYPE_JSON, MIMETYPE_PROTOBUF,
    MIMETYPE_PROTOBUF_IETF})
  public Response get(final @Context ServletContext context, final @Context UriInfo uriInfo) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("GET " + uriInfo.getAbsolutePath());
    }
    servlet.getMetrics().incrementRequests(1);
    try {
      NamespacesModel rowModel = null;
      rowModel = new NamespacesModel(servlet.getAdmin());
      servlet.getMetrics().incrementSucessfulGetRequests(1);
      return Response.ok(rowModel).build();
    } catch (IOException e) {
      servlet.getMetrics().incrementFailedGetRequests(1);
      throw new RuntimeException("Cannot retrieve list of namespaces.");
    }
  }

  /**
   * Dispatch to NamespaceInstanceResource
   */
  @Path("{namespace}")
  public NamespacesInstanceResource getNamespaceInstanceResource(
      final @PathParam("namespace") String namespace) throws IOException {
    return new NamespacesInstanceResource(namespace);
  }
}