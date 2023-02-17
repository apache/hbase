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
package org.apache.hadoop.hbase.chaos.factories;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.function.Function;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.chaos.actions.Action;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Splitter;
import org.apache.hbase.thirdparty.com.google.common.collect.Iterables;

public class ConfigurableSlowDeterministicMonkeyFactory extends SlowDeterministicMonkeyFactory {

  private static final Logger LOG =
    LoggerFactory.getLogger(ConfigurableSlowDeterministicMonkeyFactory.class);

  final static String HEAVY_ACTIONS = "heavy.actions";
  final static String TABLE_PARAM = "\\$table_name";

  @SuppressWarnings("ImmutableEnumChecker")
  public enum SupportedTypes {
    FLOAT(p -> Float.parseFloat(p)),
    LONG(p -> Long.parseLong(p)),
    INT(p -> Integer.parseInt(p)),
    TABLENAME(p -> TableName.valueOf(p));

    final Function<String, Object> converter;

    SupportedTypes(Function<String, Object> converter) {
      this.converter = converter;
    }

    Object convert(String param) {
      return converter.apply(param);
    }
  }

  @Override
  protected Action[] getHeavyWeightedActions() {
    String actions = this.properties.getProperty(HEAVY_ACTIONS);
    if (actions == null || actions.isEmpty()) {
      return super.getHeavyWeightedActions();
    } else {
      try {
        List<String> actionClasses = Splitter.on(';').splitToList(actions);
        Action[] heavyActions = new Action[actionClasses.size()];
        int i = 0;
        for (String action : actionClasses) {
          heavyActions[i++] = instantiateAction(action);
        }
        LOG.info("Created actions {}", (Object[]) heavyActions); // non-varargs call to LOG#info
        return heavyActions;
      } catch (Exception e) {
        LOG.error("Error trying to instantiate heavy actions. Returning null array.", e);
      }
      return null;
    }
  }

  private Action instantiateAction(String actionString) throws Exception {
    final String packageName = "org.apache.hadoop.hbase.chaos.actions";
    Iterable<String> classAndParams =
      Splitter.on('(').split(Iterables.get(Splitter.on(')').split(actionString), 0));
    String className = packageName + "." + Iterables.get(classAndParams, 0);
    String[] params = Splitter.on(',')
      .splitToStream(
        Iterables.get(classAndParams, 1).replaceAll(TABLE_PARAM, tableName.getNameAsString()))
      .toArray(String[]::new);
    LOG.info("About to instantiate action class: {}; With constructor params: {}", className,
      params);
    Class<? extends Action> actionClass = (Class<? extends Action>) Class.forName(className);
    Constructor<? extends Action>[] constructors =
      (Constructor<? extends Action>[]) actionClass.getDeclaredConstructors();
    for (Constructor<? extends Action> c : constructors) {
      if (c.getParameterCount() != params.length) {
        continue;
      }
      Class[] paramTypes = c.getParameterTypes();
      Object[] constructorParams = new Object[paramTypes.length];
      for (int i = 0; i < paramTypes.length; i++) {
        constructorParams[i] =
          SupportedTypes.valueOf(paramTypes[i].getSimpleName().toUpperCase()).convert(params[i]);
      }
      return c.newInstance(constructorParams);
    }
    throw new IllegalArgumentException(
      "Couldn't find any matching constructor for: " + actionString);
  }
}
