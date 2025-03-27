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
package org.apache.hadoop.hbase.http.log;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.regex.Pattern;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.http.HttpServer;
import org.apache.hadoop.hbase.logging.Log4jUtils;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.KerberosAuthenticator;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.util.ServletUtil;
import org.apache.hadoop.util.Tool;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Change log level in runtime.
 */
@InterfaceAudience.Private
public final class LogLevel {
  private static final String USAGES = "\nUsage: General options are:\n"
    + "\t[-getlevel <host:port> <classname> [-protocol (http|https)]\n"
    + "\t[-setlevel <host:port> <classname> <level> [-protocol (http|https)]";

  public static final String PROTOCOL_HTTP = "http";
  public static final String PROTOCOL_HTTPS = "https";

  public static final String READONLY_LOGGERS_CONF_KEY = "hbase.ui.logLevels.readonly.loggers";

  /**
   * A command line implementation
   */
  public static void main(String[] args) throws Exception {
    CLI cli = new CLI(new Configuration());
    System.exit(cli.run(args));
  }

  /**
   * Valid command line options.
   */
  private enum Operations {
    GETLEVEL,
    SETLEVEL,
    UNKNOWN
  }

  private static void printUsage() {
    System.err.println(USAGES);
    System.exit(-1);
  }

  public static boolean isValidProtocol(String protocol) {
    return protocol.equals(PROTOCOL_HTTP) || protocol.equals(PROTOCOL_HTTPS);
  }

  static class CLI extends Configured implements Tool {
    private Operations operation = Operations.UNKNOWN;
    private String protocol;
    private String hostName;
    private String className;
    private String level;

    CLI(Configuration conf) {
      setConf(conf);
    }

    @Override
    public int run(String[] args) throws Exception {
      try {
        parseArguments(args);
        sendLogLevelRequest();
      } catch (HadoopIllegalArgumentException e) {
        printUsage();
      }
      return 0;
    }

    /**
     * Send HTTP request to the daemon.
     * @throws HadoopIllegalArgumentException if arguments are invalid.
     * @throws Exception                      if unable to connect
     */
    private void sendLogLevelRequest() throws HadoopIllegalArgumentException, Exception {
      switch (operation) {
        case GETLEVEL:
          doGetLevel();
          break;
        case SETLEVEL:
          doSetLevel();
          break;
        default:
          throw new HadoopIllegalArgumentException("Expect either -getlevel or -setlevel");
      }
    }

    public void parseArguments(String[] args) throws HadoopIllegalArgumentException {
      if (args.length == 0) {
        throw new HadoopIllegalArgumentException("No arguments specified");
      }
      int nextArgIndex = 0;
      while (nextArgIndex < args.length) {
        switch (args[nextArgIndex]) {
          case "-getlevel":
            nextArgIndex = parseGetLevelArgs(args, nextArgIndex);
            break;
          case "-setlevel":
            nextArgIndex = parseSetLevelArgs(args, nextArgIndex);
            break;
          case "-protocol":
            nextArgIndex = parseProtocolArgs(args, nextArgIndex);
            break;
          default:
            throw new HadoopIllegalArgumentException("Unexpected argument " + args[nextArgIndex]);
        }
      }

      // if operation is never specified in the arguments
      if (operation == Operations.UNKNOWN) {
        throw new HadoopIllegalArgumentException("Must specify either -getlevel or -setlevel");
      }

      // if protocol is unspecified, set it as http.
      if (protocol == null) {
        protocol = PROTOCOL_HTTP;
      }
    }

    private int parseGetLevelArgs(String[] args, int index) throws HadoopIllegalArgumentException {
      // fail if multiple operations are specified in the arguments
      if (operation != Operations.UNKNOWN) {
        throw new HadoopIllegalArgumentException("Redundant -getlevel command");
      }
      // check number of arguments is sufficient
      if (index + 2 >= args.length) {
        throw new HadoopIllegalArgumentException("-getlevel needs two parameters");
      }
      operation = Operations.GETLEVEL;
      hostName = args[index + 1];
      className = args[index + 2];
      return index + 3;
    }

    private int parseSetLevelArgs(String[] args, int index) throws HadoopIllegalArgumentException {
      // fail if multiple operations are specified in the arguments
      if (operation != Operations.UNKNOWN) {
        throw new HadoopIllegalArgumentException("Redundant -setlevel command");
      }
      // check number of arguments is sufficient
      if (index + 3 >= args.length) {
        throw new HadoopIllegalArgumentException("-setlevel needs three parameters");
      }
      operation = Operations.SETLEVEL;
      hostName = args[index + 1];
      className = args[index + 2];
      level = args[index + 3];
      return index + 4;
    }

    private int parseProtocolArgs(String[] args, int index) throws HadoopIllegalArgumentException {
      // make sure only -protocol is specified
      if (protocol != null) {
        throw new HadoopIllegalArgumentException("Redundant -protocol command");
      }
      // check number of arguments is sufficient
      if (index + 1 >= args.length) {
        throw new HadoopIllegalArgumentException("-protocol needs one parameter");
      }
      // check protocol is valid
      protocol = args[index + 1];
      if (!isValidProtocol(protocol)) {
        throw new HadoopIllegalArgumentException("Invalid protocol: " + protocol);
      }
      return index + 2;
    }

    /**
     * Send HTTP request to get log level.
     * @throws HadoopIllegalArgumentException if arguments are invalid.
     * @throws Exception                      if unable to connect
     */
    private void doGetLevel() throws Exception {
      process(protocol + "://" + hostName + "/logLevel?log=" + className);
    }

    /**
     * Send HTTP request to set log level.
     * @throws HadoopIllegalArgumentException if arguments are invalid.
     * @throws Exception                      if unable to connect
     */
    private void doSetLevel() throws Exception {
      process(protocol + "://" + hostName + "/logLevel?log=" + className + "&level=" + level);
    }

    /**
     * Connect to the URL. Supports HTTP and supports SPNEGO authentication. It falls back to simple
     * authentication if it fails to initiate SPNEGO.
     * @param url the URL address of the daemon servlet
     * @return a connected connection
     * @throws Exception if it can not establish a connection.
     */
    private HttpURLConnection connect(URL url) throws Exception {
      AuthenticatedURL.Token token = new AuthenticatedURL.Token();
      AuthenticatedURL aUrl;
      SSLFactory clientSslFactory;
      HttpURLConnection connection;
      // If https is chosen, configures SSL client.
      if (PROTOCOL_HTTPS.equals(url.getProtocol())) {
        clientSslFactory = new SSLFactory(SSLFactory.Mode.CLIENT, this.getConf());
        clientSslFactory.init();
        SSLSocketFactory sslSocketF = clientSslFactory.createSSLSocketFactory();

        aUrl = new AuthenticatedURL(new KerberosAuthenticator(), clientSslFactory);
        connection = aUrl.openConnection(url, token);
        HttpsURLConnection httpsConn = (HttpsURLConnection) connection;
        httpsConn.setSSLSocketFactory(sslSocketF);
      } else {
        aUrl = new AuthenticatedURL(new KerberosAuthenticator());
        connection = aUrl.openConnection(url, token);
      }
      connection.connect();
      return connection;
    }

    /**
     * Configures the client to send HTTP request to the URL. Supports SPENGO for authentication.
     * @param urlString URL and query string to the daemon's web UI
     * @throws Exception if unable to connect
     */
    private void process(String urlString) throws Exception {
      URL url = new URL(urlString);
      System.out.println("Connecting to " + url);

      HttpURLConnection connection = connect(url);

      // We implement the validateResponse method inside hbase to handle for HTML response.
      // as with Jetty 12: getResponseMessage returns "Precondition Failed" vs
      // "Modification of logger protected.org.apache.hadoop.hbase.http.log.TestLogLevel is
      // disallowed in configuration" in Jetty 9
      LogLevelExceptionUtils.validateResponse(connection, 200);

      // read from the servlet

      try (
        InputStreamReader streamReader =
          new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8);
        BufferedReader bufferedReader = new BufferedReader(streamReader)) {
        bufferedReader.lines().filter(Objects::nonNull).filter(line -> line.startsWith(MARKER))
          .forEach(line -> System.out.println(TAG.matcher(line).replaceAll("")));
      } catch (IOException ioe) {
        System.err.println("" + ioe);
      }
    }
  }

  private static final String MARKER = "<!-- OUTPUT -->";
  private static final Pattern TAG = Pattern.compile("<[^>]*>");

  /**
   * A servlet implementation
   */
  @InterfaceAudience.Private
  public static class Servlet extends HttpServlet {
    private static final long serialVersionUID = 1L;

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
      // Do the authorization
      if (!HttpServer.hasAdministratorAccess(getServletContext(), request, response)) {
        return;
      }
      // Disallow modification of the LogLevel if explicitly set to readonly
      Configuration conf =
        (Configuration) getServletContext().getAttribute(HttpServer.CONF_CONTEXT_ATTRIBUTE);
      if (conf.getBoolean("hbase.master.ui.readonly", false)) {
        sendError(response, HttpServletResponse.SC_FORBIDDEN,
          "Modification of HBase via the UI is disallowed in configuration.");
        return;
      }
      response.setContentType("text/html");
      PrintWriter out;
      try {
        String headerPath = "header.jsp?pageTitle=Log Level";
        request.getRequestDispatcher(headerPath).include(request, response);
        out = response.getWriter();
      } catch (FileNotFoundException e) {
        // in case file is not found fall back to old design
        out = ServletUtil.initHTML(response, "Log Level");
      }
      out.println(FORMS);

      String logName = ServletUtil.getParameter(request, "log");
      String level = ServletUtil.getParameter(request, "level");

      String[] readOnlyLogLevels = conf.getStrings(READONLY_LOGGERS_CONF_KEY);

      if (logName != null) {
        out.println("<h2>Results</h2>");
        out.println(MARKER + "Submitted Log Name: <b>" + logName + "</b><br />");

        Logger log = LoggerFactory.getLogger(logName);
        out.println(MARKER + "Log Class: <b>" + log.getClass().getName() + "</b><br />");
        if (level != null) {
          if (!isLogLevelChangeAllowed(logName, readOnlyLogLevels)) {
            sendError(response, HttpServletResponse.SC_PRECONDITION_FAILED,
              "Modification of logger " + logName + " is disallowed in configuration.");
            return;
          }

          out.println(MARKER + "Submitted Level: <b>" + level + "</b><br />");
        }
        process(log, level, out);
      }

      try {
        String footerPath = "footer.jsp";
        out.println("</div>");
        request.getRequestDispatcher(footerPath).include(request, response);
      } catch (FileNotFoundException e) {
        out.println(ServletUtil.HTML_TAIL);
      }
      out.close();
    }

    private boolean isLogLevelChangeAllowed(String logger, String[] readOnlyLogLevels) {
      if (readOnlyLogLevels == null) {
        return true;
      }
      for (String readOnlyLogLevel : readOnlyLogLevels) {
        if (logger.startsWith(readOnlyLogLevel)) {
          return false;
        }
      }
      return true;
    }

    private void sendError(HttpServletResponse response, int code, String message)
      throws IOException {
      response.setStatus(code, message);
      response.sendError(code, message);
    }

    static final String FORMS = "<div class='container-fluid content'>\n"
      + "<div class='row inner_header top_header'>\n" + "<div class='page-header'>\n"
      + "<h1>Get/Set Log Level</h1>\n" + "</div>\n" + "</div>\n" + "\n" + "<h2>Actions</h2>\n"
      + "\n" + "<div class='row mb-4'>\n" + "<div class='col'>\n"
      + "<form class='row g-3 align-items-center justify-content-center'>\n"
      + "<div class='col-sm-auto'>\n"
      + "<button type='submit' class='btn btn-primary'>Get Log Level</button>\n" + "</div>\n"
      + "  <div class='col-sm-auto'>\n"
      + "<input type='text' name='log' class='form-control' size='50'"
      + " required='required' placeholder='Log Name (required)'>\n" + "</div>\n"
      + "  <div class='col-sm-auto'>\n"
      + "<span>Gets the current log level for the specified log name.</span>\n" + "</div>\n"
      + "</form>\n" + "</div>\n" + "</div>\n" + "\n" + "<div class='row'>\n" + "<div class='col'>\n"
      + "\n" + "<form class='row g-3 align-items-center justify-content-center'>\n"
      + "<div class='col-sm-auto'>\n"
      + "<button type='submit' class='btn btn-primary'>Set Log Level</button>\n" + "</div>\n"
      + "<div class='col-sm-auto'>\n"
      + "<input type='text' name='log' class='form-control mb-2' size='50'"
      + " required='required' placeholder='Log Name (required)'>\n"
      + "<input type='text' name='level' class='form-control' size='50'"
      + " required='required' placeholder='Log Level (required)'>\n" + "</div>\n"
      + "<div class='col-sm-auto'>\n"
      + "<span>Sets the specified log level for the specified log name.</span>\n" + "</div>\n"
      + "</form>\n" + "\n" + "</div>\n" + "</div>" + "<hr>\n";

    private static void process(Logger logger, String levelName, PrintWriter out) {
      if (levelName != null) {
        try {
          Log4jUtils.setLogLevel(logger.getName(), levelName);
          out.println(MARKER + "<div class='text-success'>" + "Setting Level to <strong>"
            + levelName + "</strong> ...<br />" + "</div>");
        } catch (IllegalArgumentException e) {
          out.println(MARKER + "<div class='text-danger'>" + "Bad level : <strong>" + levelName
            + "</strong><br />" + "</div>");
        }
      }
      out.println(MARKER + "Effective level: <b>" + Log4jUtils.getEffectiveLevel(logger.getName())
        + "</b><br />");
    }
  }

  private LogLevel() {
  }
}
