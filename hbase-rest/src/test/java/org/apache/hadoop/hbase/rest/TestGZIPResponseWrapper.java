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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.hbase.rest.filter.GZIPResponseStream;
import org.apache.hadoop.hbase.rest.filter.GZIPResponseWrapper;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestGZIPResponseWrapper {

  private final HttpServletResponse response = mock(HttpServletResponse.class);
  private final GZIPResponseWrapper wrapper = new GZIPResponseWrapper(response);
  
  /**
   * wrapper should set all headers except "content-length"
   */
  @Test
  public void testHeader() throws IOException {
    wrapper.setStatus(200);
    verify(response).setStatus(200);
    wrapper.addHeader("header", "header value");
    verify(response).addHeader("header", "header value");
    wrapper.addHeader("content-length", "header value2");
    verify(response, never()).addHeader("content-length", "header value");

    wrapper.setIntHeader("header", 5);
    verify(response).setIntHeader("header", 5);
    wrapper.setIntHeader("content-length", 4);
    verify(response, never()).setIntHeader("content-length", 4);

    wrapper.setHeader("set-header", "new value");
    verify(response).setHeader("set-header", "new value");
    wrapper.setHeader("content-length", "content length value");
    verify(response, never()).setHeader("content-length", "content length value");

    wrapper.sendRedirect("location");
    verify(response).sendRedirect("location");
    
    wrapper.flushBuffer();
    verify(response).flushBuffer();
  }

  @Test
  public void testResetBuffer() throws IOException {
    when(response.isCommitted()).thenReturn(false);
    ServletOutputStream out = mock(ServletOutputStream.class);
    when(response.getOutputStream()).thenReturn(out);

    ServletOutputStream servletOutput = wrapper.getOutputStream();
    assertEquals(GZIPResponseStream.class, servletOutput.getClass());
    wrapper.resetBuffer();
    verify(response).setHeader("Content-Encoding", null);

    when(response.isCommitted()).thenReturn(true);
    servletOutput = wrapper.getOutputStream();
    assertEquals(out.getClass(), servletOutput.getClass());
    assertNotNull(wrapper.getWriter());
  }

  @Test
  public void testReset() throws IOException {
    when(response.isCommitted()).thenReturn(false);
    ServletOutputStream out = mock(ServletOutputStream.class);
    when(response.getOutputStream()).thenReturn(out);

    ServletOutputStream servletOutput = wrapper.getOutputStream();
    verify(response).addHeader("Content-Encoding", "gzip");
    assertEquals(GZIPResponseStream.class, servletOutput.getClass());
    wrapper.reset();
    verify(response).setHeader("Content-Encoding", null);

    when(response.isCommitted()).thenReturn(true);
    servletOutput = wrapper.getOutputStream();
    assertEquals(out.getClass(), servletOutput.getClass());
  }

  @Test
  public void testSendError() throws IOException {
    wrapper.sendError(404);
    verify(response).sendError(404);

    wrapper.sendError(404, "error message");
    verify(response).sendError(404, "error message");
  }

}
