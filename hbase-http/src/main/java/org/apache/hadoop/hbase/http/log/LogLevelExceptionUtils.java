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
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * HTTP utility class to help propagate server side exception in log level servlet to the client
 * over HTTP (HTML payload) It parses HTTP client connections and recreates the exception.
 */
@InterfaceAudience.Private
public class LogLevelExceptionUtils {

  private static void throwEx(Throwable ex) {
    LogLevelExceptionUtils.<RuntimeException> throwException(ex);
  }

  @SuppressWarnings("unchecked")
  private static <E extends Throwable> void throwException(Throwable ex) throws E {
    throw (E) ex;
  }

  /**
   * Validates the status of an <code>HttpURLConnection</code> against an expected HTTP status code.
   * If the current status code is not the expected one it throws an exception with a detail message
   * using Server side error messages if available.
   * <p>
   * <b>NOTE: This is an adapted version of the original method in HttpServerUtil.java of Hadoop,
   * but we handle for HTML response.
   * @param conn           the <code>HttpURLConnection</code>.
   * @param expectedStatus the expected HTTP status code.
   * @throws IOException thrown if the current status code does not match the expected one.
   */
  @SuppressWarnings("unchecked")
  public static void validateResponse(HttpURLConnection conn, int expectedStatus)
    throws IOException {
    if (conn.getResponseCode() != expectedStatus) {
      Exception toThrow = null;

      try (InputStream es = conn.getErrorStream()) {
        if (es != null) {
          try (InputStreamReader isr = new InputStreamReader(es, StandardCharsets.UTF_8);
            BufferedReader reader = new BufferedReader(isr)) {
            final String errorAsHtml = reader.lines().collect(Collectors.joining("\n"));

            final String status = extractValue(errorAsHtml, "<th>STATUS:</th><td>(\\d+)</td>");
            final String message = extractValue(errorAsHtml, "<th>MESSAGE:</th><td>([^<]+)</td>");
            final String uri = extractValue(errorAsHtml, "<th>URI:</th><td>([^<]+)</td>");
            final String exception = extractValue(errorAsHtml, "<title>([^<]+)</title>");

            toThrow = new IOException(
              String.format("HTTP status [%s], message [%s], URL [%s], exception [%s]", status,
                message, uri, exception));
          }
        }
      } catch (Exception ex) {
        toThrow =
          new IOException(String.format("HTTP status [%d], message [%s], URL [%s], exception [%s]",
            conn.getResponseCode(), conn.getResponseMessage(), conn.getURL(), ex), ex);
      }
      if (toThrow != null) {
        throwEx(toThrow);
      }
    }
  }

  private static String extractValue(String html, String regex) {
    Pattern pattern = Pattern.compile(regex);
    Matcher matcher = pattern.matcher(html);
    if (matcher.find()) {
      return matcher.group(1);
    }
    return null;
  }

}
