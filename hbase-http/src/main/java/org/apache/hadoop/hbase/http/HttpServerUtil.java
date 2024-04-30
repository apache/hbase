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
package org.apache.hadoop.hbase.http;

import java.util.EnumSet;
import javax.servlet.DispatcherType;
import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.org.eclipse.jetty.security.ConstraintMapping;
import org.apache.hbase.thirdparty.org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.apache.hbase.thirdparty.org.eclipse.jetty.servlet.FilterHolder;
import org.apache.hbase.thirdparty.org.eclipse.jetty.servlet.ServletContextHandler;
import org.apache.hbase.thirdparty.org.eclipse.jetty.util.security.Constraint;

/**
 * HttpServer utility.
 */
@InterfaceAudience.Private
public final class HttpServerUtil {

  public static final String PATH_SPEC_ANY = "/*";

  /**
   * Add constraints to a Jetty Context to disallow undesirable Http methods.
   * @param ctxHandler         The context to modify
   * @param allowOptionsMethod if true then OPTIONS method will not be set in constraint mapping
   */
  public static void constrainHttpMethods(ServletContextHandler ctxHandler,
    boolean allowOptionsMethod) {
    Constraint c = new Constraint();
    c.setAuthenticate(true);

    ConstraintMapping cmt = new ConstraintMapping();
    cmt.setConstraint(c);
    cmt.setMethod("TRACE");
    cmt.setPathSpec("/*");

    ConstraintSecurityHandler securityHandler = new ConstraintSecurityHandler();

    if (!allowOptionsMethod) {
      ConstraintMapping cmo = new ConstraintMapping();
      cmo.setConstraint(c);
      cmo.setMethod("OPTIONS");
      cmo.setPathSpec("/*");
      securityHandler.setConstraintMappings(new ConstraintMapping[] { cmt, cmo });
    } else {
      securityHandler.setConstraintMappings(new ConstraintMapping[] { cmt });
    }

    ctxHandler.setSecurityHandler(securityHandler);
  }

  public static void addClickjackingPreventionFilter(ServletContextHandler ctxHandler,
    Configuration conf, String pathSpec) {
    FilterHolder holder = new FilterHolder();
    holder.setName("clickjackingprevention");
    holder.setClassName(ClickjackingPreventionFilter.class.getName());
    holder.setInitParameters(ClickjackingPreventionFilter.getDefaultParameters(conf));
    ctxHandler.addFilter(holder, pathSpec, EnumSet.allOf(DispatcherType.class));
  }

  public static void addSecurityHeadersFilter(ServletContextHandler ctxHandler, Configuration conf,
    boolean isSecure, String pathSpec) {
    FilterHolder holder = new FilterHolder();
    holder.setName("securityheaders");
    holder.setClassName(SecurityHeadersFilter.class.getName());
    holder.setInitParameters(SecurityHeadersFilter.getDefaultParameters(conf, isSecure));
    ctxHandler.addFilter(holder, pathSpec, EnumSet.allOf(DispatcherType.class));
  }

  private HttpServerUtil() {
  }
}
