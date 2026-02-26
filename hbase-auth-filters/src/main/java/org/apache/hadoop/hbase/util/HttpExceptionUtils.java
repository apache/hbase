/**
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
package org.apache.hadoop.hbase.util;

import org.apache.hadoop.util.JsonSerialization;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.Writer;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * HTTP utility class to help propagate server side exception to the client
 * over HTTP as a JSON payload.
 * <p>
 * It creates HTTP Servlet and JAX-RPC error responses including details of the
 * exception that allows a client to recreate the remote exception.
 * <p>
 * It parses HTTP client connections and recreates the exception.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class HttpExceptionUtils {

  public static final String ERROR_JSON = "RemoteException";
  public static final String ERROR_EXCEPTION_JSON = "exception";
  public static final String ERROR_CLASSNAME_JSON = "javaClassName";
  public static final String ERROR_MESSAGE_JSON = "message";

  private static final String APPLICATION_JSON_MIME = "application/json";

  private static final String ENTER = System.getProperty("line.separator");

  /**
   * Creates a HTTP servlet response serializing the exception in it as JSON.
   *
   * @param response the servlet response
   * @param status the error code to set in the response
   * @param ex the exception to serialize in the response
   * @throws IOException thrown if there was an error while creating the
   * response
   */
  public static void createServletExceptionResponse(
      HttpServletResponse response, int status, Throwable ex)
      throws IOException {
    response.setStatus(status);
    response.setContentType(APPLICATION_JSON_MIME);
    Map<String, Object> json = new LinkedHashMap<String, Object>();
    json.put(ERROR_MESSAGE_JSON, getOneLineMessage(ex));
    json.put(ERROR_EXCEPTION_JSON, ex.getClass().getSimpleName());
    json.put(ERROR_CLASSNAME_JSON, ex.getClass().getName());
    Map<String, Object> jsonResponse =
        Collections.singletonMap(ERROR_JSON, json);
    Writer writer = response.getWriter();
    JsonSerialization.writer().writeValue(writer, jsonResponse);
    writer.flush();
  }

  private static String getOneLineMessage(Throwable exception) {
    String message = exception.getMessage();
    if (message != null) {
      int i = message.indexOf(ENTER);
      if (i > -1) {
        message = message.substring(0, i);
      }
    }
    return message;
  }
}
