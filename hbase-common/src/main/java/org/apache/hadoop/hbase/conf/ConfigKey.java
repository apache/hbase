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
package org.apache.hadoop.hbase.conf;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for basic validation of configuration values.
 */
@InterfaceAudience.Private
public final class ConfigKey {
  private static final Logger LOG = LoggerFactory.getLogger(ConfigKey.class);

  private ConfigKey() {
  }

  @FunctionalInterface
  private interface Validator {
    void validate(Configuration conf);
  }

  // Map of configuration keys to validators
  private static final Map<String, Validator> validators = new ConcurrentHashMap<>();

  /**
   * Registers the configuration key that expects an integer.
   * @param key        configuration key
   * @param predicates additional predicates to validate the value
   * @return the key
   */
  @SafeVarargs
  public static String INT(String key, Predicate<Integer>... predicates) {
    return register(key,
      conf -> validateNumeric(key, "an integer", () -> conf.getInt(key, 0), predicates));
  }

  /**
   * Registers the configuration key that expects a long.
   * @param key        configuration key
   * @param predicates additional predicates to validate the value
   * @return the key
   */
  @SafeVarargs
  public static String LONG(String key, Predicate<Long>... predicates) {
    return register(key,
      conf -> validateNumeric(key, "a long", () -> conf.getLong(key, 0), predicates));
  }

  /**
   * Registers the configuration key that expects a float.
   * @param key        configuration key
   * @param predicates additional predicates to validate the value
   * @return the key
   */
  @SafeVarargs
  public static String FLOAT(String key, Predicate<Float>... predicates) {
    return register(key,
      conf -> validateNumeric(key, "a float", () -> conf.getFloat(key, 0), predicates));
  }

  /**
   * Registers the configuration key that expects a double.
   * @param key        configuration key
   * @param predicates additional predicates to validate the value
   * @return the key
   */
  @SafeVarargs
  public static String DOUBLE(String key, Predicate<Double>... predicates) {
    return register(key,
      conf -> validateNumeric(key, "a double", () -> conf.getDouble(key, 0), predicates));
  }

  /**
   * Registers the configuration key that expects a class.
   * @param key      configuration key
   * @param expected the expected class or interface the value should implement
   * @return the key
   */
  public static <T> String CLASS(String key, Class<T> expected) {
    return register(key, conf -> {
      String value = conf.get(key);
      try {
        if (!expected.isAssignableFrom(Class.forName(value))) {
          throw new IllegalArgumentException(
            String.format("%s ('%s') is not compatible to %s.", value, key, expected.getName()));
        }
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException(String.format("'%s' must be a class.", key), e);
      }
    });
  }

  /**
   * Registers the configuration key that expects a boolean value. We actually don't register a
   * validator, because {@link Configuration#getBoolean} doesn't throw an exception even if the
   * value is not a boolean. So this is only for documentation purposes.
   * @param key configuration key
   * @return the key
   */
  public static String BOOLEAN(String key) {
    return key;
  }

  /**
   * Validates the configuration.
   * @param conf a configuration to validate
   */
  public static void validate(Configuration conf) {
    conf.iterator().forEachRemaining(entry -> {
      Validator validator = validators.get(entry.getKey());
      if (validator != null) {
        validator.validate(conf);
      }
    });
  }

  @FunctionalInterface
  private interface NumberGetter<T> {
    T get() throws NumberFormatException;
  }

  private static <T> void validateNumeric(String name, String expected, NumberGetter<T> getter,
    Predicate<T>... predicates) {
    try {
      T value = getter.get();
      for (Predicate<T> predicate : predicates) {
        if (!predicate.test(value)) {
          throw new IllegalArgumentException("Invalid value for '" + name + "': " + value + ".");
        }
      }
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(String.format("'%s' must be %s.", name, expected), e);
    }
  }

  private static String register(String key, Validator validator) {
    LOG.debug("Registering config validator for {}", key);
    validators.put(key, validator);
    return key;
  }
}
