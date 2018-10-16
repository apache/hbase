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

package org.apache.hadoop.hbase.rest.client;

import java.io.IOException;
import java.io.InputStream;

import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.http.Header;
import org.apache.http.HttpResponse;

/**
 * The HTTP result code, response headers, and body of a HTTP response.
 */
@InterfaceAudience.Public
public class Response {
  private static final Logger LOG = LoggerFactory.getLogger(Response.class);

  private int code;
  private Header[] headers;
  private byte[] body;
  private HttpResponse resp;
  private InputStream stream;

  /**
   * Constructor
   * @param code the HTTP response code
   */
  public Response(int code) {
    this(code, null, null);
  }

  /**
   * Constructor
   * @param code the HTTP response code
   * @param headers the HTTP response headers
   */
  public Response(int code, Header[] headers) {
    this(code, headers, null);
  }

  /**
   * Constructor
   * @param code the HTTP response code
   * @param headers the HTTP response headers
   * @param body the response body, can be null
   */
  public Response(int code, Header[] headers, byte[] body) {
    this.code = code;
    this.headers = headers;
    this.body = body;
  }
  
  /**
   * Constructor
   * @param code the HTTP response code
   * @param headers headers the HTTP response headers
   * @param resp the response
   * @param in Inputstream if the response had one.
   * Note: this is not thread-safe
   */
  public Response(int code, Header[] headers, HttpResponse resp, InputStream in) {
    this.code = code;
    this.headers = headers;
    this.body = null;
    this.resp = resp;
    this.stream = in;
  }

  /**
   * @return the HTTP response code
   */
  public int getCode() {
    return code;
  }
  
  /**
   * Gets the input stream instance.
   *
   * @return an instance of InputStream class.
   */
  public InputStream getStream(){
    return this.stream;
  }

  /**
   * @return the HTTP response headers
   */
  public Header[] getHeaders() {
    return headers;
  }

  public String getHeader(String key) {
    for (Header header: headers) {
      if (header.getName().equalsIgnoreCase(key)) {
        return header.getValue();
      }
    }
    return null;
  }

  /**
   * @return the value of the Location header
   */
  public String getLocation() {
    return getHeader("Location");
  }

  /**
   * @return true if a response body was sent
   */
  public boolean hasBody() {
    return body != null;
  }

  /**
   * @return the HTTP response body
   */
  public byte[] getBody() {
    if (body == null) {
      try {
        body = Client.getResponseBody(resp);
      } catch (IOException ioe) {
        LOG.debug("encountered ioe when obtaining body", ioe);
      }
    }
    return body;
  }

  /**
   * @param code the HTTP response code
   */
  public void setCode(int code) {
    this.code = code;
  }

  /**
   * @param headers the HTTP response headers
   */
  public void setHeaders(Header[] headers) {
    this.headers = headers;
  }

  /**
   * @param body the response body
   */
  public void setBody(byte[] body) {
    this.body = body;
  }
}
