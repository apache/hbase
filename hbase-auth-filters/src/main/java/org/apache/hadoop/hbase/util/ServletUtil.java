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

import java.io.*;
import java.util.Calendar;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ServletUtil {
  /**
   * Initial HTML header.
   *
   * @param response response.
   * @param title title.
   * @throws IOException raised on errors performing I/O.
   * @return PrintWriter.
   */
  public static PrintWriter initHTML(ServletResponse response, String title
      ) throws IOException {
    response.setContentType("text/html");
    PrintWriter out = response.getWriter();
    out.println("<html>\n"
        + "<link rel='stylesheet' type='text/css' href='/static/hadoop.css'>\n"
        + "<title>" + title + "</title>\n"
        + "<body>\n"
        + "<h1>" + title + "</h1>\n");
    return out;
  }

  /**
   * Get a parameter from a ServletRequest.
   * Return null if the parameter contains only white spaces.
   *
   * @param request request.
   * @param name name.
   * @return get a parameter from a ServletRequest.
   */
  public static String getParameter(ServletRequest request, String name) {
    String s = request.getParameter(name);
    if (s == null) {
      return null;
    }
    s = s.trim();
    return s.length() == 0? null: s;
  }
  
  /**
   * parseLongParam.
   *
   * @param request request.
   * @param param param.
   * @return a long value as passed in the given parameter, throwing
   * an exception if it is not present or if it is not a valid number.
   * @throws IOException raised on errors performing I/O.
   */
  public static long parseLongParam(ServletRequest request, String param)
      throws IOException {
    String paramStr = request.getParameter(param);
    if (paramStr == null) {
      throw new IOException("Invalid request has no " + param + " parameter");
    }
    
    return Long.parseLong(paramStr);
  }

  public static final String HTML_TAIL = "<hr />\n"
    + "<a href='http://hadoop.apache.org'>Hadoop</a>, "
    + Calendar.getInstance().get(Calendar.YEAR) + ".\n"
    + "</body></html>";

  /**
   * HTML footer to be added in the jsps.
   * @return the HTML footer.
   */
  public static String htmlFooter() {
    return HTML_TAIL;
  }

  /**
   * Parse the path component from the given request and return w/o decoding.
   * @param request Http request to parse
   * @param servletName the name of servlet that precedes the path
   * @return path component, null if the default charset is not supported
   */
  public static String getRawPath(final HttpServletRequest request, String servletName) {
    Preconditions.checkArgument(request.getRequestURI().startsWith(servletName+"/"));
    return request.getRequestURI().substring(servletName.length());
  }
}
