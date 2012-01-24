/*
 * Copyright The Apache Software Foundation
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

import javax.ws.rs.QueryParam;
import javax.ws.rs.PathParam;
import javax.ws.rs.Path;
import javax.ws.rs.Encoded;

public class CheckAndDeleteTableResource extends ResourceBase {

  String table;
  
  /**
   * Constructor
   * 
   * @param table
   * @throws IOException
   */
  public CheckAndDeleteTableResource(String table) throws IOException {
    super();
    this.table = table;
  }
  
  /** @return the table name */
  String getName() {
    return table;
  }

  @Path("{rowspec: .+}")
  public CheckAndDeleteRowResource getCheckAndDeleteRowResource(
      // We need the @Encoded decorator so Jersey won't urldecode before
      // the RowSpec constructor has a chance to parse
      final @PathParam("rowspec") @Encoded String rowspec,
      final @QueryParam("v") String versions) throws IOException {
    return new CheckAndDeleteRowResource(this, rowspec, versions);
  }
}

