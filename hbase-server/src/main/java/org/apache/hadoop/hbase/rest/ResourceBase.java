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

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;
import org.apache.hadoop.util.StringUtils;

@InterfaceAudience.Private
public class ResourceBase implements Constants {

  RESTServlet servlet;
  Class<?>  accessDeniedClazz;

  public ResourceBase() throws IOException {
    servlet = RESTServlet.getInstance();
    try {
      accessDeniedClazz = Class.forName("org.apache.hadoop.hbase.security.AccessDeniedException");
    } catch (ClassNotFoundException e) {
    }
  }
  
  protected Response processException(Throwable exp) {
    Throwable curr = exp;
    if(accessDeniedClazz != null) {
      //some access denied exceptions are buried
      while (curr != null) {
        if(accessDeniedClazz.isAssignableFrom(curr.getClass())) {
          throw new WebApplicationException(
              Response.status(Response.Status.FORBIDDEN)
                .type(MIMETYPE_TEXT).entity("Forbidden" + CRLF +
                   StringUtils.stringifyException(exp) + CRLF)
                .build());
        }
        curr = curr.getCause();
      }
    }
    //TableNotFound may also be buried one level deep
    if (exp instanceof TableNotFoundException ||
        exp.getCause() instanceof TableNotFoundException) {
      throw new WebApplicationException(
        Response.status(Response.Status.NOT_FOUND)
          .type(MIMETYPE_TEXT).entity("Not found" + CRLF +
             StringUtils.stringifyException(exp) + CRLF)
          .build());
    }
    if (exp instanceof NoSuchColumnFamilyException){
      throw new WebApplicationException(
        Response.status(Response.Status.NOT_FOUND)
          .type(MIMETYPE_TEXT).entity("Not found" + CRLF +
             StringUtils.stringifyException(exp) + CRLF)
          .build());
    }
    if (exp instanceof RuntimeException) {
      throw new WebApplicationException(
          Response.status(Response.Status.BAD_REQUEST)
            .type(MIMETYPE_TEXT).entity("Bad request" + CRLF +
              StringUtils.stringifyException(exp) + CRLF)
            .build());
    }
    if (exp instanceof RetriesExhaustedWithDetailsException) {
      RetriesExhaustedWithDetailsException retryException =
          (RetriesExhaustedWithDetailsException) exp;
      processException(retryException.getCause(0));
    }
    throw new WebApplicationException(
      Response.status(Response.Status.SERVICE_UNAVAILABLE)
        .type(MIMETYPE_TEXT).entity("Unavailable" + CRLF +
          StringUtils.stringifyException(exp) + CRLF)
        .build());
  }
}
