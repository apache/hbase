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

#include "core/hbase_configuration_loader.h"

#include <glog/logging.h>
#include <boost/foreach.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/xml_parser.hpp>

namespace hbase {

bool is_valid_bool(const std::string &raw) {
  if (raw.empty()) {
    return false;
  }

  if (!strcasecmp(raw.c_str(), "true")) {
    return true;
  }
  if (!strcasecmp(raw.c_str(), "false")) {
    return true;
  }
  return false;
}

bool str_to_bool(const std::string &raw) {
  if (!strcasecmp(raw.c_str(), "true")) {
    return true;
  }
  return false;
}

HBaseConfigurationLoader::HBaseConfigurationLoader() {}

HBaseConfigurationLoader::~HBaseConfigurationLoader() {}

void HBaseConfigurationLoader::SetDefaultSearchPath() {
  /*
   * Try (in order, taking the first valid one):
   * $HBASE_CONF_DIR
   * /etc/hbase/conf
   *
   */
  const char *hadoop_conf_dir_env = getenv("HBASE_CONF");
  if (hadoop_conf_dir_env) {
    AddToSearchPath(hadoop_conf_dir_env);
  } else {
    AddToSearchPath(kHBaseDefauktConfPath);
  }
}

void HBaseConfigurationLoader::ClearSearchPath() { search_paths_.clear(); }

void HBaseConfigurationLoader::SetSearchPath(const std::string &search_path) {
  search_paths_.clear();

  std::vector<std::string> paths;
  std::string::size_type start = 0;
  std::string::size_type end = search_path.find(kSearchPathSeparator);

  while (end != std::string::npos) {
    paths.push_back(search_path.substr(start, end - start));
    start = ++end;
    end = search_path.find(kSearchPathSeparator, start);
  }
  paths.push_back(search_path.substr(start, search_path.length()));

  for (auto path : paths) {
    AddToSearchPath(path);
  }
}

void HBaseConfigurationLoader::AddToSearchPath(const std::string &search_path) {
  if (search_path.empty()) return;

  std::string path_to_add(search_path);
  if (search_path.back() != kFileSeparator) {
    path_to_add += kFileSeparator;
  }
  if (std::find(search_paths_.begin(), search_paths_.end(), path_to_add) ==
      search_paths_.end())
    search_paths_.push_back(path_to_add);
}

void HBaseConfigurationLoader::AddDefaultResources() {
  resources_.push_back(kHBaseDefaultXml);
  resources_.push_back(kHBaseSiteXml);
}

void HBaseConfigurationLoader::AddResources(const std::string &filename) {
  if (std::find(resources_.begin(), resources_.end(), filename) ==
      resources_.end())
    resources_.push_back(filename);
}

optional<Configuration> HBaseConfigurationLoader::LoadDefaultResources() {
  SetDefaultSearchPath();
  AddDefaultResources();
  ConfigMap conf_property;
  bool success = false;
  for (auto dir : search_paths_) {
    for (auto file : resources_) {
      std::string config_file = dir + file;
      std::ifstream stream(config_file);
      if (stream.is_open()) {
        success |= LoadProperties(config_file, conf_property);
      } else {
        DLOG(WARNING) << "Unable to open file[" << config_file << "]";
      }
    }
  }
  if (success) {
    return std::experimental::make_optional<Configuration>(conf_property);
  } else {
    return optional<Configuration>();
  }
}

optional<Configuration> HBaseConfigurationLoader::LoadResources(
    const std::string &search_path, const std::vector<std::string> &resources) {
  SetSearchPath(search_path);
  for (const auto &resource : resources) AddResources(resource);
  ConfigMap conf_property;
  bool success = false;
  for (auto dir : search_paths_) {
    for (auto file : resources_) {
      std::string config_file = dir + file;
      std::ifstream stream(config_file);
      if (stream.is_open()) {
        success |= LoadProperties(config_file, conf_property);
      } else {
        DLOG(WARNING) << "Unable to open file[" << config_file << "]";
      }
    }
  }
  if (success) {
    return std::experimental::make_optional<Configuration>(conf_property);
  } else {
    return optional<Configuration>();
  }
}

bool HBaseConfigurationLoader::LoadProperties(const std::string &file,
                                              ConfigMap &property_map) {
  // Create empty property tree object
  using boost::property_tree::ptree;
  ptree pt;
  try {
    // Load XML file and put contents in a property tree.
    // If read fails, throw exception.
    read_xml(file, pt);

    // If configuration key is not found exception is thrown
    std::string configuration = pt.get<std::string>("configuration");

    // Iterate over configuration section.
    // Store all found properties in ConfigMap
    BOOST_FOREACH (ptree::value_type &v, pt.get_child("configuration")) {
      if ("property" == v.first) {
        std::string name_node = v.second.get<std::string>("name");
        std::string value_node = v.second.get<std::string>("value");
        if ((name_node.size() > 0) && (value_node.size() > 0)) {
          boost::optional<std::string> final_node =
              v.second.get_optional<std::string>("final");
          UpdateMapWithValue(property_map, name_node, value_node, final_node);
        }
      }
    }
  } catch (std::exception &ex) {
    DLOG(WARNING) << "Exception in parsing file [" << file << "]:[" << ex.what()
                  << "]";
    return false;
  }
  return true;
}

bool HBaseConfigurationLoader::UpdateMapWithValue(
    ConfigMap &map, const std::string &key, const std::string &value,
    boost::optional<std::string> final_text) {
  auto map_value = map.find(key);
  if (map_value != map.end() && map_value->second.final) {
    return false;
  }

  bool final_value = false;
  if (nullptr != final_text.get_ptr()) {
    if (is_valid_bool(final_text.get())) {
      final_value = str_to_bool(final_text.get());
    }
  }

  map[key].value = value;
  map[key].final = final_value;
  return true;
}
} /* namespace hbase */
