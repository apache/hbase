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

#pragma once

#include <map>
#include <string>
#include <vector>

#include <boost/optional.hpp>
#include <experimental/optional>

#include "core/configuration.h"

namespace hbase {

template <class T>
using optional = std::experimental::optional<T>;

class HBaseConfigurationLoader {
 public:
  HBaseConfigurationLoader();
  ~HBaseConfigurationLoader();

  /**
   * @brief Creates a Configuration object based on default resources loaded
   * from default search paths. Default search path will be either $HBASE_CONF
   * is set or /etc/hbase/conf. Default resources are hbase-default.xml and
   * hbase-site.xml.SetDefaultSearchPath() and AddDefaultResources() are used
   * for the same.
   * Values are loaded in from hbase-default.xml first and then from
   * hbase-site.xml.
   * Properties in hbase-site.xml will override the ones in hbase-default.xml
   * unless marked as final
   */
  optional<Configuration> LoadDefaultResources();

  /*
   * @brief Creates a Configuration object based on resources loaded from search
   * paths. Search paths are defined in search_path. Values are loaded from
   * resources and will be overridden unless marked as final
   * @param search_path - ':' search paths to load resources.
   * @param resources - list of resources used to load configuration properties.
   */
  optional<Configuration> LoadResources(
      const std::string &search_path,
      const std::vector<std::string> &resources);

 private:
  using ConfigMap = Configuration::ConfigMap;
  const std::string kHBaseDefaultXml = "hbase-default.xml";
  const std::string kHBaseSiteXml = "hbase-site.xml";
  const std::string kHBaseDefauktConfPath = "/etc/hbase/conf";

  // Adds FILE_SEPARATOR to the search path
  const char kFileSeparator = '/';

  // Separator using which multiple search paths can be defined.
  const char kSearchPathSeparator = ':';

  /**
   * List of paths which will be looked up for loading properties.
   */
  std::vector<std::string> search_paths_;

  /**
   * List of files which will be looked up in search_paths_ to load properties.
   */
  std::vector<std::string> resources_;

  /**
   * @brief This method sets the search path to the default search path (i.e.
   * "$HBASE_CONF" or "/etc/hbase/conf" if HBASE_CONF is absent)
   */
  void SetDefaultSearchPath();

  /**
   * @brief Clears out the set search path(s)
   */
  void ClearSearchPath();

  /**
   * @brief Sets the search path to ":"-delimited paths, clearing already
   * defined values
   * @param search_path Single path or ":"-delimited separated paths
   */
  void SetSearchPath(const std::string &search_path);

  /**
   * @brief Adds an element to the search path if not already present.
   * @param search_path Path that will be added to load config values
   */
  void AddToSearchPath(const std::string &search_path);

  /**
   * @brief This method will add default resources i.e. hbase-default.xml and
   * hbase-site.xml to the default search path.
   */
  void AddDefaultResources();

  /**
   * @brief Adds resources to list for loading config values.
   * @param filename to be added to resources.
   */
  void AddResources(const std::string &filename);

  /**
   * @brief Loads properties in file identified by file in a map identified by
   * property_map
   * @param file XML file which defines HBase configuration properties.
   * @param property_map Property map representing HBase configuration as key
   * value pairs.
   * @throws Boost ptree exception if some parsing issue occurs.
   */
  bool LoadProperties(const std::string &file, ConfigMap &property_map);

  /**
   * @brief This method will create a map of the name and properties.
   * @param map Property map to hold configuration properties.
   * @param key value of name node.
   * @param value value of value node.
   * @param final_text value of final node true or false if present
   */
  bool UpdateMapWithValue(ConfigMap &map, const std::string &key,
                          const std::string &value,
                          boost::optional<std::string> final_text);
};

} /* namespace hbase */
