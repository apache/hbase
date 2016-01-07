/**
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

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.rest.filter.GZIPResponseStream;
import org.apache.hadoop.hbase.rest.filter.GZIPResponseWrapper;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

@Category(MediumTests.class)
public class TestGZIPResponseWrapper {

  /**
   * headers function should be called in response except header "content-length"
   * 
   * @throws IOException
   */
  @Test
  public void testHeader() throws IOException {

    HttpServletResponse response = mock(HttpServletResponse.class);

    GZIPResponseWrapper test = new GZIPResponseWrapper(response);
    test.setStatus(200);
    verify(response).setStatus(200);
    test.addHeader("header", "header value");
    verify(response).addHeader("header", "header value");
    test.addHeader("content-length", "header value2");
    verify(response, never()).addHeader("content-length", "header value");

    test.setIntHeader("header", 5);
    verify(response).setIntHeader("header", 5);
    test.setIntHeader("content-length", 4);
    verify(response, never()).setIntHeader("content-length", 4);

    test.setHeader("set-header", "new value");
    verify(response).setHeader("set-header", "new value");
    test.setHeader("content-length", "content length value");
    verify(response, never()).setHeader("content-length", "content length value");

    test.sendRedirect("location");
    verify(response).sendRedirect("location");
    
    test.flushBuffer();
    verify(response).flushBuffer();

  }

  @Test
  public void testResetBuffer() throws IOException {
    HttpServletResponse response = mock(HttpServletResponse.class);
    when(response.isCommitted()).thenReturn(false);
    ServletOutputStream out = mock(ServletOutputStream.class);
    when(response.getOutputStream()).thenReturn(out);
    GZIPResponseWrapper test = new GZIPResponseWrapper(response);

    ServletOutputStream servletOutput = test.getOutputStream();
    assertEquals(org.apache.hadoop.hbase.rest.filter.GZIPResponseStream.class,
        servletOutput.getClass());
    test.resetBuffer();
    verify(response).setHeader("Content-Encoding", null);

    when(response.isCommitted()).thenReturn(true);
    servletOutput = test.getOutputStream();
    assertEquals(out.getClass(), servletOutput.getClass());
    assertNotNull(test.getWriter());

  }

  @Test
  public void testReset() throws IOException {
    HttpServletResponse response = mock(HttpServletResponse.class);
    when(response.isCommitted()).thenReturn(false);
    ServletOutputStream out = mock(ServletOutputStream.class);
    when(response.getOutputStream()).thenReturn(out);
    GZIPResponseWrapper test = new GZIPResponseWrapper(response);

    ServletOutputStream servletOutput = test.getOutputStream();
    assertEquals(org.apache.hadoop.hbase.rest.filter.GZIPResponseStream.class,
        servletOutput.getClass());
    test.reset();
    verify(response).setHeader("Content-Encoding", null);

    when(response.isCommitted()).thenReturn(true);
    servletOutput = test.getOutputStream();
    assertEquals(out.getClass(), servletOutput.getClass());
  }

  @Test
  public void testSendError() throws IOException {
    HttpServletResponse response = mock(HttpServletResponse.class);
    GZIPResponseWrapper test = new GZIPResponseWrapper(response);

    test.sendError(404);
    verify(response).sendError(404);

    test.sendError(404, "error message");
    verify(response).sendError(404, "error message");

  }

  @Test
  public void testGZIPResponseStream() throws IOException {
    HttpServletResponse httpResponce = mock(HttpServletResponse.class);
    ServletOutputStream out = mock(ServletOutputStream.class);

    when(httpResponce.getOutputStream()).thenReturn(out);
    GZIPResponseStream test = new GZIPResponseStream(httpResponce);
    verify(httpResponce).addHeader("Content-Encoding", "gzip");

    test.close();

    test.resetBuffer();
    verify(httpResponce).setHeader("Content-Encoding", null);
  }
}
