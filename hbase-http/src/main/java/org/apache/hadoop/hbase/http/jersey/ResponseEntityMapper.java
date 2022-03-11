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
package org.apache.hadoop.hbase.http.jersey;

import java.io.IOException;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hbase.thirdparty.javax.ws.rs.container.ContainerRequestContext;
import org.apache.hbase.thirdparty.javax.ws.rs.container.ContainerResponseContext;
import org.apache.hbase.thirdparty.javax.ws.rs.container.ContainerResponseFilter;
import org.apache.hbase.thirdparty.javax.ws.rs.core.Response.Status;

/**
 * Generate a uniform response wrapper around the Entity returned from the resource.
 * @see <a href="https://jsonapi.org/format/#document-top-level">JSON API Document Structure</a>
 * @see <a href="https://jsonapi.org/format/#error-objects">JSON API Error Objects</a>
 */
@InterfaceAudience.Private
public class ResponseEntityMapper implements ContainerResponseFilter {

  @Override
  public void filter(
    ContainerRequestContext requestContext,
    ContainerResponseContext responseContext
  ) throws IOException {
    /*
     * Follows very loosely the top-level document specification described in by JSON API. Only
     * handles 200 response codes; leaves room for errors and other response types.
     */

    final int statusCode = responseContext.getStatus();
    if (Status.OK.getStatusCode() != statusCode) {
      return;
    }

    responseContext.setEntity(ImmutableMap.of("data", responseContext.getEntity()));
  }
}
