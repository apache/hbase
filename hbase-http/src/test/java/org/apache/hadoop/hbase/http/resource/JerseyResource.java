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
package org.apache.hadoop.hbase.http.resource;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.javax.ws.rs.DefaultValue;
import org.apache.hbase.thirdparty.javax.ws.rs.GET;
import org.apache.hbase.thirdparty.javax.ws.rs.Path;
import org.apache.hbase.thirdparty.javax.ws.rs.PathParam;
import org.apache.hbase.thirdparty.javax.ws.rs.Produces;
import org.apache.hbase.thirdparty.javax.ws.rs.QueryParam;
import org.apache.hbase.thirdparty.javax.ws.rs.core.MediaType;
import org.apache.hbase.thirdparty.javax.ws.rs.core.Response;
import org.apache.hbase.thirdparty.org.eclipse.jetty.util.ajax.JSON;

/**
 * A simple Jersey resource class TestHttpServer. The servlet simply puts the path and the op
 * parameter in a map and return it in JSON format in the response.
 */
@Path("")
public class JerseyResource {
  private static final Logger LOG = LoggerFactory.getLogger(JerseyResource.class);

  public static final String PATH = "path";
  public static final String OP = "op";

  @GET
  @Path("{" + PATH + ":.*}")
  @Produces({ MediaType.APPLICATION_JSON })
  public Response get(@PathParam(PATH) @DefaultValue("UNKNOWN_" + PATH) final String path,
    @QueryParam(OP) @DefaultValue("UNKNOWN_" + OP) final String op) throws IOException {
    LOG.info("get: " + PATH + "=" + path + ", " + OP + "=" + op);

    final Map<String, Object> m = new TreeMap<>();
    m.put(PATH, path);
    m.put(OP, op);
    final String js = new JSON().toJSON(m);
    return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
  }
}
