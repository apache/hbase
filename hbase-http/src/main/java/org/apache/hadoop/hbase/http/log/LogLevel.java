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
package org.apache.hadoop.hbase.http.log;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URL;
import java.net.URLConnection;
import java.util.regex.Pattern;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.logging.impl.Jdk14Logger;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.hbase.http.HttpServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ServletUtil;
import org.apache.log4j.LogManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.Log4jLoggerAdapter;

/**
 * Change log level in runtime.
 */
@InterfaceAudience.Private
public final class LogLevel {
  public static final String USAGES = "\nUsage: General options are:\n"
      + "\t[-getlevel <host:httpPort> <name>]\n"
      + "\t[-setlevel <host:httpPort> <name> <level>]\n";

  /**
   * A command line implementation
   */
  public static void main(String[] args) {
    if (args.length == 3 && "-getlevel".equals(args[0])) {
      process("http://" + args[1] + "/logLevel?log=" + args[2]);
      return;
    }
    else if (args.length == 4 && "-setlevel".equals(args[0])) {
      process("http://" + args[1] + "/logLevel?log=" + args[2]
              + "&level=" + args[3]);
      return;
    }

    System.err.println(USAGES);
    System.exit(-1);
  }

  private static void process(String urlstring) {
    try {
      URL url = new URL(urlstring);
      System.out.println("Connecting to " + url);
      URLConnection connection = url.openConnection();
      connection.connect();
      try (InputStreamReader streamReader = new InputStreamReader(connection.getInputStream());
           BufferedReader bufferedReader = new BufferedReader(streamReader)) {
        for(String line; (line = bufferedReader.readLine()) != null; ) {
          if (line.startsWith(MARKER)) {
            System.out.println(TAG.matcher(line).replaceAll(""));
          }
        }
      }
    } catch (IOException ioe) {
      System.err.println("" + ioe);
    }
  }

  static final String MARKER = "<!-- OUTPUT -->";
  static final Pattern TAG = Pattern.compile("<[^>]*>");

  /**
   * A servlet implementation
   */
  @InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
  @InterfaceStability.Unstable
  public static class Servlet extends HttpServlet {
    private static final long serialVersionUID = 1L;

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response
        ) throws ServletException, IOException {

      // Do the authorization
      if (!HttpServer.hasAdministratorAccess(getServletContext(), request,
          response)) {
        return;
      }
      // Disallow modification of the LogLevel if explicitly set to readonly
      Configuration conf = (Configuration) getServletContext().getAttribute(
          HttpServer.CONF_CONTEXT_ATTRIBUTE);
      if (conf.getBoolean("hbase.master.ui.readonly", false)) {
        response.sendError(HttpServletResponse.SC_FORBIDDEN, "Modification of HBase via"
            + " the UI is disallowed in configuration.");
        return;
      }

      PrintWriter out = ServletUtil.initHTML(response, "Log Level");
      String logName = ServletUtil.getParameter(request, "log");
      String level = ServletUtil.getParameter(request, "level");

      if (logName != null) {
        out.println("<br /><hr /><h3>Results</h3>");
        out.println(MARKER
            + "Submitted Log Name: <b>" + logName + "</b><br />");

        Logger log = LoggerFactory.getLogger(logName);
        out.println(MARKER
            + "Log Class: <b>" + log.getClass().getName() +"</b><br />");
        if (level != null) {
          out.println(MARKER + "Submitted Level: <b>" + level + "</b><br />");
        }

        if (log instanceof Log4JLogger) {
          process(((Log4JLogger)log).getLogger(), level, out);
        } else if (log instanceof Jdk14Logger) {
          process(((Jdk14Logger)log).getLogger(), level, out);
        } else if (log instanceof Log4jLoggerAdapter) {
          process(LogManager.getLogger(logName), level, out);
        } else {
          out.println("Sorry, " + log.getClass() + " not supported.<br />");
        }
      }

      out.println(FORMS);
      out.println(ServletUtil.HTML_TAIL);
    }

    static final String FORMS = "\n<br /><hr /><h3>Get / Set</h3>"
        + "\n<form>Log: <input type='text' size='50' name='log' /> "
        + "<input type='submit' value='Get Log Level' />"
        + "</form>"
        + "\n<form>Log: <input type='text' size='50' name='log' /> "
        + "Level: <input type='text' name='level' /> "
        + "<input type='submit' value='Set Log Level' />"
        + "</form>";

    private static void process(org.apache.log4j.Logger log, String level,
        PrintWriter out) throws IOException {
      if (level != null) {
        if (!level.equals(org.apache.log4j.Level.toLevel(level).toString())) {
          out.println(MARKER + "Bad level : <b>" + level + "</b><br />");
        } else {
          log.setLevel(org.apache.log4j.Level.toLevel(level));
          out.println(MARKER + "Setting Level to " + level + " ...<br />");
        }
      }
      out.println(MARKER
          + "Effective level: <b>" + log.getEffectiveLevel() + "</b><br />");
    }

    private static void process(java.util.logging.Logger log, String level,
        PrintWriter out) throws IOException {
      if (level != null) {
        log.setLevel(java.util.logging.Level.parse(level));
        out.println(MARKER + "Setting Level to " + level + " ...<br />");
      }

      java.util.logging.Level lev;
      for(; (lev = log.getLevel()) == null; log = log.getParent());
      out.println(MARKER + "Effective level: <b>" + lev + "</b><br />");
    }
  }

  private LogLevel() {}
}
