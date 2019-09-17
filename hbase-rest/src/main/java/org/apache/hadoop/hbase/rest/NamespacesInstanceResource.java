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
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.rest.model.NamespacesInstanceModel;
import org.apache.hadoop.hbase.rest.model.TableListModel;
import org.apache.hadoop.hbase.rest.model.TableModel;

/**
 * Implements the following REST end points:
 * <p>
 * <tt>/namespaces/{namespace} GET: get namespace properties.</tt>
 * <tt>/namespaces/{namespace} POST: create namespace.</tt>
 * <tt>/namespaces/{namespace} PUT: alter namespace.</tt>
 * <tt>/namespaces/{namespace} DELETE: drop namespace.</tt>
 * <tt>/namespaces/{namespace}/tables GET: list namespace's tables.</tt>
 * <p>
 */
@InterfaceAudience.Private
public class NamespacesInstanceResource extends ResourceBase {

  private static final Logger LOG = LoggerFactory.getLogger(NamespacesInstanceResource.class);
  String namespace;
  boolean queryTables = false;

  /**
   * Constructor for standard NamespaceInstanceResource.
   * @throws IOException
   */
  public NamespacesInstanceResource(String namespace) throws IOException {
    this(namespace, false);
  }

  /**
   * Constructor for querying namespace table list via NamespaceInstanceResource.
   * @throws IOException
   */
  public NamespacesInstanceResource(String namespace, boolean queryTables) throws IOException {
    super();
    this.namespace = namespace;
    this.queryTables = queryTables;
  }

  /**
   * Build a response for GET namespace description or GET list of namespace tables.
   * @param context servlet context
   * @param uriInfo (JAX-RS context variable) request URL
   * @return A response containing NamespacesInstanceModel for a namespace descriptions and
   * TableListModel for a list of namespace tables.
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

    // Respond to list of namespace tables requests.
    if(queryTables){
      TableListModel tableModel = new TableListModel();
      try{
        HTableDescriptor[] tables = servlet.getAdmin().listTableDescriptorsByNamespace(namespace);
        for(int i = 0; i < tables.length; i++){
          tableModel.add(new TableModel(tables[i].getTableName().getQualifierAsString()));
        }

        servlet.getMetrics().incrementSucessfulGetRequests(1);
        return Response.ok(tableModel).build();
      }catch(IOException e) {
        servlet.getMetrics().incrementFailedGetRequests(1);
        throw new RuntimeException("Cannot retrieve table list for '" + namespace + "'.");
      }
    }

    // Respond to namespace description requests.
    try {
      NamespacesInstanceModel rowModel =
          new NamespacesInstanceModel(servlet.getAdmin(), namespace);
      servlet.getMetrics().incrementSucessfulGetRequests(1);
      return Response.ok(rowModel).build();
    } catch (IOException e) {
      servlet.getMetrics().incrementFailedGetRequests(1);
      throw new RuntimeException("Cannot retrieve info for '" + namespace + "'.");
    }
  }

  /**
   * Build a response for PUT alter namespace with properties specified.
   * @param model properties used for alter.
   * @param uriInfo (JAX-RS context variable) request URL
   * @return response code.
   */
  @PUT
  @Consumes({MIMETYPE_XML, MIMETYPE_JSON, MIMETYPE_PROTOBUF,
    MIMETYPE_PROTOBUF_IETF})
  public Response put(final NamespacesInstanceModel model, final @Context UriInfo uriInfo) {
    return processUpdate(model, true, uriInfo);
  }

  /**
   * Build a response for POST create namespace with properties specified.
   * @param model properties used for create.
   * @param uriInfo (JAX-RS context variable) request URL
   * @return response code.
   */
  @POST
  @Consumes({MIMETYPE_XML, MIMETYPE_JSON, MIMETYPE_PROTOBUF,
    MIMETYPE_PROTOBUF_IETF})
  public Response post(final NamespacesInstanceModel model,
      final @Context UriInfo uriInfo) {
    return processUpdate(model, false, uriInfo);
  }


  // Check that POST or PUT is valid and then update namespace.
  private Response processUpdate(NamespacesInstanceModel model, final boolean updateExisting,
      final UriInfo uriInfo) {
    if (LOG.isTraceEnabled()) {
      LOG.trace((updateExisting ? "PUT " : "POST ") + uriInfo.getAbsolutePath());
    }
    if (model == null) {
      try {
        model = new NamespacesInstanceModel(namespace);
      } catch(IOException ioe) {
        servlet.getMetrics().incrementFailedPutRequests(1);
        throw new RuntimeException("Cannot retrieve info for '" + namespace + "'.");
      }
    }
    servlet.getMetrics().incrementRequests(1);

    if (servlet.isReadOnly()) {
      servlet.getMetrics().incrementFailedPutRequests(1);
      return Response.status(Response.Status.FORBIDDEN).type(MIMETYPE_TEXT)
          .entity("Forbidden" + CRLF).build();
    }

    Admin admin = null;
    boolean namespaceExists = false;
    try {
      admin = servlet.getAdmin();
      namespaceExists = doesNamespaceExist(admin, namespace);
    }catch (IOException e) {
      servlet.getMetrics().incrementFailedPutRequests(1);
      return processException(e);
    }

    // Do not allow creation if namespace already exists.
    if(!updateExisting && namespaceExists){
      servlet.getMetrics().incrementFailedPutRequests(1);
      return Response.status(Response.Status.FORBIDDEN).type(MIMETYPE_TEXT).
          entity("Namespace '" + namespace + "' already exists.  Use REST PUT " +
          "to alter the existing namespace.").build();
    }

    // Do not allow altering if namespace does not exist.
    if (updateExisting && !namespaceExists){
      servlet.getMetrics().incrementFailedPutRequests(1);
      return Response.status(Response.Status.FORBIDDEN).type(MIMETYPE_TEXT).
          entity("Namespace '" + namespace + "' does not exist. Use " +
          "REST POST to create the namespace.").build();
    }

    return createOrUpdate(model, uriInfo, admin, updateExisting);
  }

  // Do the actual namespace create or alter.
  private Response createOrUpdate(final NamespacesInstanceModel model, final UriInfo uriInfo,
      final Admin admin, final boolean updateExisting) {
    NamespaceDescriptor.Builder builder = NamespaceDescriptor.create(namespace);
    builder.addConfiguration(model.getProperties());
    if(model.getProperties().size() > 0){
      builder.addConfiguration(model.getProperties());
    }
    NamespaceDescriptor nsd = builder.build();

    try{
      if(updateExisting){
        admin.modifyNamespace(nsd);
      }else{
        admin.createNamespace(nsd);
      }
    }catch (IOException e) {
      servlet.getMetrics().incrementFailedPutRequests(1);
      return processException(e);
    }

    servlet.getMetrics().incrementSucessfulPutRequests(1);

    return updateExisting ? Response.ok(uriInfo.getAbsolutePath()).build() :
      Response.created(uriInfo.getAbsolutePath()).build();
  }

  private boolean doesNamespaceExist(Admin admin, String namespaceName) throws IOException{
    NamespaceDescriptor[] nd = admin.listNamespaceDescriptors();
    for(int i = 0; i < nd.length; i++){
      if(nd[i].getName().equals(namespaceName)){
        return true;
      }
    }
    return false;
  }

  /**
   * Build a response for DELETE delete namespace.
   * @param message value not used.
   * @param headers value not used.
   * @return response code.
   */
  @DELETE
  public Response deleteNoBody(final byte[] message,
      final @Context UriInfo uriInfo, final @Context HttpHeaders headers) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("DELETE " + uriInfo.getAbsolutePath());
    }
    if (servlet.isReadOnly()) {
      servlet.getMetrics().incrementFailedDeleteRequests(1);
      return Response.status(Response.Status.FORBIDDEN).type(MIMETYPE_TEXT)
          .entity("Forbidden" + CRLF).build();
    }

    try{
      Admin admin = servlet.getAdmin();
      if (!doesNamespaceExist(admin, namespace)){
        return Response.status(Response.Status.NOT_FOUND).type(MIMETYPE_TEXT).
            entity("Namespace '" + namespace + "' does not exists.  Cannot " +
            "drop namespace.").build();
      }

      admin.deleteNamespace(namespace);
      servlet.getMetrics().incrementSucessfulDeleteRequests(1);
      return Response.ok().build();

    } catch (IOException e) {
      servlet.getMetrics().incrementFailedDeleteRequests(1);
      return processException(e);
    }
  }

  /**
   * Dispatch to NamespaceInstanceResource for getting list of tables.
   */
  @Path("tables")
  public NamespacesInstanceResource getNamespaceInstanceResource(
      final @PathParam("tables") String namespace) throws IOException {
    return new NamespacesInstanceResource(this.namespace, true);
  }
}
