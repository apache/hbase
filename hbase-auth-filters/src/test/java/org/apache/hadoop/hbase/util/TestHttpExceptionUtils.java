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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;

public class TestHttpExceptionUtils {

  @Test
  public void testCreateServletException() throws IOException {
    StringWriter writer = new StringWriter();
    PrintWriter printWriter = new PrintWriter(writer);
    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
    Mockito.when(response.getWriter()).thenReturn(printWriter);
    int status = HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
    Exception ex = new IOException("Hello IOEX");
    HttpExceptionUtils.createServletExceptionResponse(response, status, ex);
    Mockito.verify(response).setStatus(status);
    Mockito.verify(response).setContentType(Mockito.eq("application/json"));
    ObjectMapper mapper = new ObjectMapper();
    Map json = mapper.readValue(writer.toString(), Map.class);
    json = (Map) json.get(HttpExceptionUtils.ERROR_JSON);
    Assert.assertEquals(IOException.class.getName(),
        json.get(HttpExceptionUtils.ERROR_CLASSNAME_JSON));
    Assert.assertEquals(IOException.class.getSimpleName(),
        json.get(HttpExceptionUtils.ERROR_EXCEPTION_JSON));
    Assert.assertEquals("Hello IOEX",
        json.get(HttpExceptionUtils.ERROR_MESSAGE_JSON));
  }
}
