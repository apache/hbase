/**
 * Copyright 2011 The Apache Software Foundation
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
package org.apache.hadoop.hbase.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * Helper class that uses reflection to iterate through a class and find
 * "pretty print" annotations of methods.  It constructs the pretty print
 * classes, and allows you to easily call getMap given the method and params
 */
public class ParamFormatHelper<TContext> {
  Map<Method, ParamFormatter<TContext>> paramFormatters =
      new HashMap<Method, ParamFormatter<TContext>>();

  public static final Log LOG = LogFactory.getLog(ParamFormatHelper.class);

  TContext context;

  /**
   * General constructor which allows you to specify the context, clazz and
   * formatType individually
   * @param context the context to use when calling ParamFormatter instances
   * @param clazz the Class to reflect over to get the ParamFormat annotations
   * @param formatType which formatTypes to use (ignores others)
   */
  public ParamFormatHelper(TContext context, Class<?> clazz,
                           ParamFormat.FormatTypes formatType) {
    this.context = context;

    for (Method method : clazz.getMethods()) {
      for (Annotation ann : method.getAnnotations()) {

        if (!(ann instanceof ParamFormat)) continue;
        ParamFormat paramFormat = (ParamFormat) ann;
        if (paramFormat.formatType() != formatType) continue;

        try {
          // getMethod throws if it wasn't derived from ParamFormatter<TContext>
          paramFormat.clazz().getMethod("getMap", Object[].class, clazz);

          // The generic constraint in the annotation checks this...
          @SuppressWarnings("unchecked")
          ParamFormatter<TContext> newFormatter =
              (ParamFormatter<TContext>) paramFormat.clazz().newInstance();

          paramFormatters.put(method, newFormatter);
        }
        catch(InstantiationException e) {
          String msg = "Can not create " + ((ParamFormat) ann).clazz().getName() +
              " make sure it's public (and static if nested)";
          throw new RuntimeException(msg, e);
        }
        catch(NoSuchMethodException e) {
          // This exception is thrown if the class clazz was derived
          //  from a different TContext
        }
        catch (IllegalAccessException e) {
          String msg = "Can not create " + ((ParamFormat) ann).clazz().getName() +
              " make sure it's public (and static if nested)";
          throw new RuntimeException(msg, e);
        }
      }
    }

  }

  /**
   * Default constructor that uses context as the context and class, and
   * DEFAULT formatType
   * @param context the context to use for calling ParamFormatter instances
   *                and for getting the ParamFormat annotations from
   */
  public ParamFormatHelper(TContext context) {
    this(context, context.getClass(), ParamFormat.FormatTypes.DEFAULT);
  }

  /**
   * Try to get info about a method given it's params
   * @param method which method was called
   * @param params the params which the method was called with
   * @return map of information about the call
   */
  public Map<String, Object> getMap(Method method, Object[] params) {
    ParamFormatter<TContext> formatter = paramFormatters.get(method);
    if (formatter == null) return null;
    return formatter.getMap(params, context);
  }
}
