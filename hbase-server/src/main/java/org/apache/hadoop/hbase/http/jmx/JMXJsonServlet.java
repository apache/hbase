/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.http.jmx;

import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.RuntimeErrorException;
import javax.management.RuntimeMBeanException;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.http.HttpServer;
import org.apache.hadoop.hbase.util.JSONBean;

/*
 * This servlet is based off of the JMXProxyServlet from Tomcat 7.0.14. It has
 * been rewritten to be read only and to output in a JSON format so it is not
 * really that close to the original.
 */
/**
 * Provides Read only web access to JMX.
 * <p>
 * This servlet generally will be placed under the /jmx URL for each
 * HttpServer.  It provides read only
 * access to JMX metrics.  The optional <code>qry</code> parameter
 * may be used to query only a subset of the JMX Beans.  This query
 * functionality is provided through the
 * {@link MBeanServer#queryNames(ObjectName, javax.management.QueryExp)}
 * method.
 * </p>
 * <p>
 * For example <code>http://.../jmx?qry=Hadoop:*</code> will return
 * all hadoop metrics exposed through JMX.
 * </p>
 * <p>
 * The optional <code>get</code> parameter is used to query an specific
 * attribute of a JMX bean.  The format of the URL is
 * <code>http://.../jmx?get=MXBeanName::AttributeName</code>
 * </p>
 * <p>
 * For example
 * <code>
 * http://../jmx?get=Hadoop:service=NameNode,name=NameNodeInfo::ClusterId
 * </code> will return the cluster id of the namenode mxbean.
 * </p>
 * <p>
 * If the <code>qry</code> or the <code>get</code> parameter is not formatted 
 * correctly then a 400 BAD REQUEST http response code will be returned. 
 * </p>
 * <p>
 * If a resouce such as a mbean or attribute can not be found, 
 * a 404 SC_NOT_FOUND http response code will be returned. 
 * </p>
 * <p>
 * The return format is JSON and in the form
 * </p>
 *  <pre><code>
 *  {
 *    "beans" : [
 *      {
 *        "name":"bean-name"
 *        ...
 *      }
 *    ]
 *  }
 *  </code></pre>
 *  <p>
 *  The servlet attempts to convert the the JMXBeans into JSON. Each
 *  bean's attributes will be converted to a JSON object member.
 *
 *  If the attribute is a boolean, a number, a string, or an array
 *  it will be converted to the JSON equivalent.
 *
 *  If the value is a {@link CompositeData} then it will be converted
 *  to a JSON object with the keys as the name of the JSON member and
 *  the value is converted following these same rules.
 *
 *  If the value is a {@link TabularData} then it will be converted
 *  to an array of the {@link CompositeData} elements that it contains.
 *
 *  All other objects will be converted to a string and output as such.
 *
 *  The bean's name and modelerType will be returned for all beans.
 *
 *  Optional paramater "callback" should be used to deliver JSONP response.
 * </p>
 *  
 */
public class JMXJsonServlet extends HttpServlet {
  private static final Log LOG = LogFactory.getLog(JMXJsonServlet.class);

  private static final long serialVersionUID = 1L;

  private static final String CALLBACK_PARAM = "callback";
  /**
   * If query string includes 'description', then we will emit bean and attribute descriptions to
   * output IFF they are not null and IFF the description is not the same as the attribute name:
   * i.e. specify an URL like so: /jmx?description=true
   */
  private static final String INCLUDE_DESCRIPTION = "description";

  /**
   * MBean server.
   */
  protected transient MBeanServer mBeanServer;

  protected transient JSONBean jsonBeanWriter;

  /**
   * Initialize this servlet.
   */
  @Override
  public void init() throws ServletException {
    // Retrieve the MBean server
    mBeanServer = ManagementFactory.getPlatformMBeanServer();
    this.jsonBeanWriter = new JSONBean();
  }

  /**
   * Process a GET request for the specified resource.
   *
   * @param request
   *          The servlet request we are processing
   * @param response
   *          The servlet response we are creating
   */
  @Override
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="XSS_REQUEST_PARAMETER_TO_SERVLET_WRITER",
    justification="TODO: See HBASE-15122")
  public void doGet(HttpServletRequest request, HttpServletResponse response) {
    try {
      if (!HttpServer.isInstrumentationAccessAllowed(getServletContext(), request, response)) {
        return;
      }
      String jsonpcb = null;
      PrintWriter writer = null;
      JSONBean.Writer beanWriter = null;
      try {
        writer = response.getWriter();
        beanWriter = this.jsonBeanWriter.open(writer);
        // "callback" parameter implies JSONP outpout
        jsonpcb = request.getParameter(CALLBACK_PARAM);
        if (jsonpcb != null) {
          response.setContentType("application/javascript; charset=utf8");
          writer.write(jsonpcb + "(");
        } else {
          response.setContentType("application/json; charset=utf8");
        }
        // Should we output description on each attribute and bean?
        String tmpStr = request.getParameter(INCLUDE_DESCRIPTION);
        boolean description = tmpStr != null && tmpStr.length() > 0;

        // query per mbean attribute
        String getmethod = request.getParameter("get");
        if (getmethod != null) {
          String[] splitStrings = getmethod.split("\\:\\:");
          if (splitStrings.length != 2) {
            beanWriter.write("result", "ERROR");
            beanWriter.write("message", "query format is not as expected.");
            beanWriter.flush();
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            return;
          }
          if (beanWriter.write(this.mBeanServer, new ObjectName(splitStrings[0]),
              splitStrings[1], description) != 0) {
            beanWriter.flush();
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
          }
          return;
        }

        // query per mbean
        String qry = request.getParameter("qry");
        if (qry == null) {
          qry = "*:*";
        }
        if (beanWriter.write(this.mBeanServer, new ObjectName(qry), null, description) != 0) {
          beanWriter.flush();
          response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        }
      } finally {
        if (beanWriter != null) beanWriter.close();
        if (jsonpcb != null) {
           writer.write(");");
        }
        if (writer != null) {
          writer.close();
        }
      }
    } catch (IOException e) {
      LOG.error("Caught an exception while processing JMX request", e);
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    } catch (MalformedObjectNameException e) {
      LOG.error("Caught an exception while processing JMX request", e);
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
    }
  }
}
