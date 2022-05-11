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
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.RegionTooBusyException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * This subclass of {@link org.apache.hadoop.hbase.client.RetriesExhaustedException} is thrown when
 * we have more information about which rows were causing which exceptions on what servers. You can
 * call {@link #mayHaveClusterIssues()} and if the result is false, you have input error problems,
 * otherwise you may have cluster issues. You can iterate over the causes, rows and last known
 * server addresses via {@link #getNumExceptions()} and {@link #getCause(int)}, {@link #getRow(int)}
 * and {@link #getHostnamePort(int)}.
 */
@SuppressWarnings("serial")
@InterfaceAudience.Public
public class RetriesExhaustedWithDetailsException extends RetriesExhaustedException {
  List<Throwable> exceptions;
  List<Row> actions;
  List<String> hostnameAndPort;

  public RetriesExhaustedWithDetailsException(final String msg) {
    super(msg);
  }

  public RetriesExhaustedWithDetailsException(final String msg, final IOException e) {
    super(msg, e);
  }

  public RetriesExhaustedWithDetailsException(List<Throwable> exceptions, List<Row> actions,
    List<String> hostnameAndPort) {
    super("Failed " + exceptions.size() + " action" + pluralize(exceptions) + ": "
      + getDesc(exceptions, actions, hostnameAndPort));

    this.exceptions = exceptions;
    this.actions = actions;
    this.hostnameAndPort = hostnameAndPort;
  }

  public List<Throwable> getCauses() {
    return exceptions;
  }

  public int getNumExceptions() {
    return exceptions.size();
  }

  public Throwable getCause(int i) {
    return exceptions.get(i);
  }

  public Row getRow(int i) {
    return actions.get(i);
  }

  public String getHostnamePort(final int i) {
    return this.hostnameAndPort.get(i);
  }

  public boolean mayHaveClusterIssues() {
    boolean res = false;

    // If all of the exceptions are DNRIOE not exception
    for (Throwable t : exceptions) {
      if (!(t instanceof DoNotRetryIOException)) {
        res = true;
      }
    }
    return res;
  }

  public static String pluralize(Collection<?> c) {
    return pluralize(c.size());
  }

  public static String pluralize(int c) {
    return c > 1 ? "s" : "";
  }

  public static String getDesc(List<Throwable> exceptions, List<? extends Row> actions,
    List<String> hostnamePort) {
    String s = getDesc(classifyExs(exceptions));
    StringBuilder addrs = new StringBuilder(s);
    addrs.append("servers with issues: ");
    Set<String> uniqAddr = new HashSet<>(hostnamePort);

    for (String addr : uniqAddr) {
      addrs.append(addr).append(", ");
    }
    return uniqAddr.isEmpty() ? addrs.toString() : addrs.substring(0, addrs.length() - 2);
  }

  public String getExhaustiveDescription() {
    StringWriter errorWriter = new StringWriter();
    PrintWriter pw = new PrintWriter(errorWriter);
    for (int i = 0; i < this.exceptions.size(); ++i) {
      Throwable t = this.exceptions.get(i);
      Row action = this.actions.get(i);
      String server = this.hostnameAndPort.get(i);
      pw.append("exception");
      if (this.exceptions.size() > 1) {
        pw.append(" #" + i);
      }
      pw.append(" from " + server + " for "
        + ((action == null) ? "unknown key" : Bytes.toStringBinary(action.getRow())));
      if (t != null) {
        pw.println();
        t.printStackTrace(pw);
      }
    }
    pw.flush();
    return errorWriter.toString();
  }

  public static Map<String, Integer> classifyExs(List<Throwable> ths) {
    Map<String, Integer> cls = new HashMap<>();
    for (Throwable t : ths) {
      if (t == null) continue;
      String name = "";
      if (t instanceof DoNotRetryIOException || t instanceof RegionTooBusyException) {
        // If RegionTooBusyException, print message since it has Region name in it.
        // RegionTooBusyException message was edited to remove variance. Has regionname, server,
        // and why the exception; no longer has duration it waited on lock nor current memsize.
        name = t.getMessage();
      } else {
        name = t.getClass().getSimpleName();
      }
      Integer i = cls.get(name);
      if (i == null) {
        i = 0;
      }
      i += 1;
      cls.put(name, i);
    }
    return cls;
  }

  public static String getDesc(Map<String, Integer> classificaton) {
    StringBuilder classificatons = new StringBuilder(11);
    for (Map.Entry<String, Integer> e : classificaton.entrySet()) {
      classificatons.append(e.getKey());
      classificatons.append(": ");
      classificatons.append(e.getValue());
      classificatons.append(" time");
      classificatons.append(pluralize(e.getValue()));
      classificatons.append(", ");
    }
    return classificatons.toString();
  }
}
