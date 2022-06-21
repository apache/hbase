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
package org.apache.hadoop.hbase.rest;

import java.io.IOException;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.javax.ws.rs.GET;
import org.apache.hbase.thirdparty.javax.ws.rs.Produces;
import org.apache.hbase.thirdparty.javax.ws.rs.core.CacheControl;
import org.apache.hbase.thirdparty.javax.ws.rs.core.Context;
import org.apache.hbase.thirdparty.javax.ws.rs.core.Response;
import org.apache.hbase.thirdparty.javax.ws.rs.core.Response.ResponseBuilder;
import org.apache.hbase.thirdparty.javax.ws.rs.core.UriInfo;

@InterfaceAudience.Private
public class ExistsResource extends ResourceBase {

  static CacheControl cacheControl;
  static {
    cacheControl = new CacheControl();
    cacheControl.setNoCache(true);
    cacheControl.setNoTransform(false);
  }

  TableResource tableResource;

  /**
   * Constructor nn
   */
  public ExistsResource(TableResource tableResource) throws IOException {
    super();
    this.tableResource = tableResource;
  }

  @GET
  @Produces({ MIMETYPE_TEXT, MIMETYPE_XML, MIMETYPE_JSON, MIMETYPE_PROTOBUF, MIMETYPE_PROTOBUF_IETF,
    MIMETYPE_BINARY })
  public Response get(final @Context UriInfo uriInfo) {
    try {
      if (!tableResource.exists()) {
        return Response.status(Response.Status.NOT_FOUND).type(MIMETYPE_TEXT)
          .entity("Not found" + CRLF).build();
      }
    } catch (IOException e) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).type(MIMETYPE_TEXT)
        .entity("Unavailable" + CRLF).build();
    }
    ResponseBuilder response = Response.ok();
    response.cacheControl(cacheControl);
    return response.build();
  }
}
