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
 *
 */

/* Some pieces of code have been added from HDFS-8707 */
#pragma once

#include <map>
#include <string>
#include <vector>

#include <experimental/optional>

namespace hbase {

template <class T>
using optional = std::experimental::optional<T>;

class Configuration {
 public:
  ~Configuration();

  /**
   * @brief Returns value identified by key in ConfigMap else default value if
   * property is absent.
   * @param key Property whose value is to be fetched. SubstituteVars will be
   * called for any variable expansion.
   */
  std::string Get(const std::string &key, const std::string &default_value) const;

  /**
   * @brief Returns int32_t identified by key in ConfigMap else default value if
   * property is absent.
   * @param key Property whose value is to be fetched. Internally Get(key) will
   * be called.
   * @throws std::runtime_error if conversion to int32_t fails
   */
  int32_t GetInt(const std::string &key, int32_t default_value) const;

  /**
   * @brief Returns int64_t identified by key in ConfigMap else default value if
   * property is absent.
   * @param key Property whose value is to be fetched. Internally Get(key) will
   * be called and
   * @throws std::runtime_error if conversion to int64_t fails
   */
  int64_t GetLong(const std::string &key, int64_t default_value) const;

  /**
   * @brief Returns double identified by key in ConfigMap else default value if
   * property is absent.
   * @param key Property whose value is to be fetched. Internally Get(key) will
   * be called.
   * @throws std::runtime_error if conversion to double fails
   */
  double GetDouble(const std::string &key, double default_value) const;

  /**
   * @brief Returns bool identified by key in ConfigMap else default value if
   * property is absent.
   * @param key Property whose value is to be fetched. Internally Get(key) will
   * be called.
   * @throws std::runtime_error if conversion to bool fails
   */
  bool GetBool(const std::string &key, bool default_value) const;

 private:
  friend class HBaseConfigurationLoader;

  /* Transparent data holder for property values */
  /* Same as Jira HDFS-8707 */
  struct ConfigData {
    std::string value;
    bool final;
    ConfigData() : final(false) {}
    explicit ConfigData(const std::string &value) : value(value), final(false) {}
    void operator=(const std::string &new_value) {
      value = new_value;
      final = false;
    }
  };

  /**
   * @brief Map which hold configuration properties.
   */
  using ConfigMap = std::map<std::string, ConfigData>;

  /**
   * @brief Create Configuration object using config_map;
   * @param config_map - Map consisting of properties.
   */
  explicit Configuration(ConfigMap &config_map);

  // Property map filled all the properties loaded by ConfigurationLoader
  ConfigMap hb_property_;

  // Variable expansion depth
  const int kMaxSubsts = 20;

  /**
   * @brief returns value for a property identified by key in ConfigMap.
   * SubstituteVars will be called for any variable expansion.
   * @param key Key whose value is to be fetched.
   */
  optional<std::string> Get(const std::string &key) const;

  /**
   * @brief returns optional int32_t value identified by the key in ConfigMap.
   * Get(key) is called to get the string value which is then converted to
   * int32_t using boost::lexical_cast.
   * @param key Key whose value is to be fetched.
   * @throws std::runtime_error if conversion to int32_t fails
   */
  optional<int32_t> GetInt(const std::string &key) const;

  /**
   * @brief returns optional int64_t value identified by the key in ConfigMap.
   * Get(key) is called internally to get the string value which is then
   * converted to int64_t using boost::lexical_cast.
   * @param key Key whose value is to be fetched.
   * @throws std::runtime_error if conversion to int64_t fails
   */
  optional<int64_t> GetLong(const std::string &key) const;

  /**
   * @brief returns optional double value identified by the key in ConfigMap.
   * Get(key) is called to get the string value which is then converted to
   * double using boost::lexical_cast.
   * @param key Key whose value is to be fetched.
   * @throws std::runtime_error if conversion to double fails
   */
  optional<double> GetDouble(const std::string &key) const;

  /**
   * @brief returns optional bool for a property identified by key in ConfigMap.
   * Get(key) is called to get the string value which is then converted to bool
   * by checking the validity.
   * @param key Key whose value is to be fetched. Get(key) is called to get the
   * string value which is then converted to bool.
   * @throws std::runtime_error if conversion to bool fails
   */
  optional<bool> GetBool(const std::string &key) const;

  /**
   * @brief This method will perform any variable expansion if present.
   * @param expression expression string to perform substitutions on.
   * @throws std::runtime_error if MAX_SUBSTS is exhausted and any more
   * substitutions are reqd to be performed.
   */
  std::string SubstituteVars(const std::string &expr) const;

  /**
   * @brief This method will check if variable expansion has to be performed or
   * not. Expression will be checked for presence of "${" and "}" to perform
   * variable expansion.
   * @param expr Expression on which will be checked for validity.
   * @param sub_variable Extracted variable from expr which will be checked
   * against environment value or ConfigMap values.
   */
  size_t IsSubVariable(const std::string &expr, std::string &sub_variable) const;

  /**
   * @brief This method will fetch value for key from environment if present.
   * @param key key to be fetched from environment.
   */
  optional<std::string> GetEnv(const std::string &key) const;

  /**
   * @brief This method will fetch value for key from ConfigMap if present.
   * @param key key to be fetched from environment.
   */
  optional<std::string> GetProperty(const std::string &key) const;
};
} /* namespace hbase */
