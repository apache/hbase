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
package org.apache.hadoop.hbase.regionserver.http;

import static org.apache.hadoop.hbase.regionserver.http.RSRequestServlet.PARAM_TOP_N;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category({ RegionServerTests.class, SmallTests.class })
public class TestRSRequestServlet {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRSRequestServlet.class);

  @Test
  public void testParseInt() throws IOException {
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

    when(request.getParameter(PARAM_TOP_N)).thenReturn("10");
    assertEquals(Integer.valueOf(10),
      RSRequestServlet.parseInt(PARAM_TOP_N, 10, 100, request, response));

    when(request.getParameter(PARAM_TOP_N)).thenReturn("100");
    assertEquals(Integer.valueOf(100),
      RSRequestServlet.parseInt(PARAM_TOP_N, 10, 100, request, response));

    when(request.getParameter(PARAM_TOP_N)).thenReturn("101");
    assertNull(RSRequestServlet.parseInt(PARAM_TOP_N, 10, 100, request, response));
    verify(response).sendError(eq(HttpServletResponse.SC_BAD_REQUEST), anyString());

    Mockito.reset(response);
    when(request.getParameter(PARAM_TOP_N)).thenReturn("0");
    assertNull(RSRequestServlet.parseInt(PARAM_TOP_N, 1, 100, request, response));
    verify(response).sendError(eq(HttpServletResponse.SC_BAD_REQUEST), anyString());
  }
}
