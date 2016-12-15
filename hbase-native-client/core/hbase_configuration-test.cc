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

#include <fstream>
#include <iostream>

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <boost/filesystem.hpp>
#include "core/configuration.h"
#include "core/hbase_configuration_loader.h"

using namespace hbase;

const std::string kDefHBaseConfPath(
    "./build/test-data/hbase-configuration-test/conf/");
const std::string kHBaseConfPath(
    "./build/test-data/hbase-configuration-test/custom-conf/");

const std::string kHBaseDefaultXml("hbase-default.xml");
const std::string kHBaseSiteXml("hbase-site.xml");

const std::string kHBaseDefaultXmlData(
    "<?xml version=\"1.0\"?>\n<?xml-stylesheet type=\"text/xsl\" "
    "href=\"configuration.xsl\"?>\n<!--\n/**\n *\n * Licensed to the Apache "
    "Software Foundation (ASF) under one\n * or more contributor license "
    "agreements.  See the NOTICE file\n * distributed with this work for "
    "additional information\n * regarding copyright ownership.  The ASF "
    "licenses this file\n * to you under the Apache License, Version 2.0 "
    "(the\n * \"License\"); you may not use this file except in compliance\n * "
    "with the License.  You may obtain a copy of the License at\n *\n *     "
    "http://www.apache.org/licenses/LICENSE-2.0\n *\n * Unless required by "
    "applicable law or agreed to in writing, software\n * distributed under "
    "the License is distributed on an \"AS IS\" BASIS,\n * WITHOUT WARRANTIES "
    "OR CONDITIONS OF ANY KIND, either express or implied.\n * See the License "
    "for the specific language governing permissions and\n * limitations under "
    "the License.\n "
    "*/\n-->\n<configuration>\n\n<property>\n<name>hbase.rootdir</"
    "name>\n<value>/root/hbase-docker/apps/hbase/data</value>\n<final>true</"
    "final>\n</"
    "property>\n\n<property>\n<name>hbase.zookeeper.property.datadir</"
    "name>\n<value>This value will be "
    "overwritten</value>\n<final>false</final>\n</"
    "property>\n\n<property>\n<name>default-prop</name>\n<value>default-value</"
    "value>\n</property>\n\n</configuration>");
const std::string kHBaseSiteXmlData(
    "<?xml version=\"1.0\"?>\n<?xml-stylesheet type=\"text/xsl\" "
    "href=\"configuration.xsl\"?>\n<!--\n/**\n *\n * Licensed to the Apache "
    "Software Foundation (ASF) under one\n * or more contributor license "
    "agreements.  See the NOTICE file\n * distributed with this work for "
    "additional information\n * regarding copyright ownership.  The ASF "
    "licenses this file\n * to you under the Apache License, Version 2.0 "
    "(the\n * \"License\"); you may not use this file except in compliance\n * "
    "with the License.  You may obtain a copy of the License at\n *\n *     "
    "http://www.apache.org/licenses/LICENSE-2.0\n *\n * Unless required by "
    "applicable law or agreed to in writing, software\n * distributed under "
    "the License is distributed on an \"AS IS\" BASIS,\n * WITHOUT WARRANTIES "
    "OR CONDITIONS OF ANY KIND, either express or implied.\n * See the License "
    "for the specific language governing permissions and\n * limitations under "
    "the License.\n "
    "*/\n-->\n<configuration>\n\n<property>\n<name>hbase.rootdir</"
    "name>\n<value>This value will not be be "
    "overwritten</value>\n</"
    "property>\n\n<property>\n<name>hbase.zookeeper.property.datadir</"
    "name>\n<value>/root/hbase-docker/zookeeper</value>\n</"
    "property>\n\n<property>\n<name>hbase-client.user.name</"
    "name>\n<value>${user.name}</value>\n</"
    "property>\n\n<property>\n<name>hbase-client.user.dir</"
    "name>\n<value>${user.dir}</value>\n</"
    "property>\n\n<property>\n<name>hbase-client.user.home</"
    "name>\n<value>${user.home}</value>\n</"
    "property>\n\n<property>\n<name>selfRef</name>\n<value>${selfRef}</"
    "value>\n</property>\n\n<property>\n<name>foo.substs</"
    "name>\n<value>${bar},${bar},${bar},${bar},${bar},${bar},${bar},${bar},${"
    "bar},${bar},</value>\n</"
    "property>\n\n<property>\n<name>foo.substs.exception</"
    "name>\n<value>${bar},${bar},${bar},${bar},${bar},${bar},${bar},${bar},${"
    "bar},${bar},${bar},${bar},${bar},${bar},${bar},${bar},${bar},${bar},${bar}"
    ",${bar},${bar}</value>\n</property>\n\n<property>\n<name>bar</"
    "name>\n<value>bar-value</value>\n</"
    "property>\n\n<property>\n<name>custom-prop</name>\n<value>custom-value</"
    "value>\n</property>\n\n<property>\n<name>int</name>\n<value>16000</"
    "value>\n</property>\n\n<property>\n<name>int.largevalue</"
    "name>\n<value>2147483646</value>\n</"
    "property>\n\n<property>\n<name>int.exception</name>\n<value>2147483648</"
    "value>\n</property>\n\n<property>\n<name>long</name>\n<value>2147483850</"
    "value>\n</property>\n\n<property>\n<name>long.largevalue</"
    "name>\n<value>9223372036854775807</value>\n</"
    "property>\n\n<property>\n<name>long.exception</"
    "name>\n<value>9223372036854775810</value>\n</"
    "property>\n\n<property>\n<name>double</name>\n<value>17.9769e+100</"
    "value>\n</property>\n\n<property>\n<name>double.largevalue</"
    "name>\n<value>170.769e+200</value>\n</"
    "property>\n\n<property>\n<name>double.exception</"
    "name>\n<value>1.79769e+310</value>\n</"
    "property>\n\n<property>\n<name>bool.true</name>\n<value>true</value>\n</"
    "property>\n\n<property>\n<name>bool.false</name>\n<value>false</value>\n</"
    "property>\n\n<property>\n<name>bool.exception</name>\n<value>unknown "
    "bool</value>\n</property>\n\n</configuration>");

void WriteDataToFile(const std::string &file, const std::string &xml_data) {
  std::ofstream hbase_conf;
  hbase_conf.open(file.c_str());
  hbase_conf << xml_data;
  hbase_conf.close();
}

void CreateHBaseConf(const std::string &dir, const std::string &file,
                     const std::string xml_data) {
  // Directory will be created if not present
  if (!boost::filesystem::exists(dir)) {
    boost::filesystem::create_directories(dir);
  }
  // Remove temp file always
  boost::filesystem::remove((dir + file).c_str());
  WriteDataToFile((dir + file), xml_data);
}

void CreateHBaseConfWithEnv() {
  CreateHBaseConf(kDefHBaseConfPath, kHBaseDefaultXml, kHBaseDefaultXmlData);
  CreateHBaseConf(kDefHBaseConfPath, kHBaseSiteXml, kHBaseSiteXmlData);
  setenv("HBASE_CONF", kDefHBaseConfPath.c_str(), 1);
}

/*
 * Config will be loaded from $HBASE_CONF. We set it @ kDefHBaseConfPath
 * Config values will be loaded from hbase-default.xml and hbase-site.xml
 * present in the above path.
 */
TEST(Configuration, LoadConfFromDefaultLocation) {
  // Remove already configured env if present.
  unsetenv("HBASE_CONF");
  CreateHBaseConf(kDefHBaseConfPath, kHBaseDefaultXml, kHBaseDefaultXmlData);
  CreateHBaseConf(kDefHBaseConfPath, kHBaseSiteXml, kHBaseSiteXmlData);
  setenv("HBASE_CONF", kDefHBaseConfPath.c_str(), 0);

  HBaseConfigurationLoader loader;
  hbase::optional<Configuration> conf = loader.LoadDefaultResources();
  ASSERT_TRUE(conf) << "No configuration object present.";
  EXPECT_STREQ((*conf).Get("custom-prop", "Set this value").c_str(),
               "custom-value");
  EXPECT_STREQ((*conf).Get("default-prop", "Set this value").c_str(),
               "default-value");
}

/*
 * Config will be loaded from hbase-site.xml defined at
 * kHBaseConfPath
 */
TEST(Configuration, LoadConfFromCustomLocation) {
  // Remove already configured env if present.
  unsetenv("HBASE_CONF");
  CreateHBaseConf(kHBaseConfPath, kHBaseSiteXml, kHBaseSiteXmlData);

  HBaseConfigurationLoader loader;
  std::vector<std::string> resources{kHBaseSiteXml};
  hbase::optional<Configuration> conf =
      loader.LoadResources(kHBaseConfPath, resources);
  ASSERT_TRUE(conf) << "No configuration object present.";
  EXPECT_STREQ((*conf).Get("custom-prop", "").c_str(), "custom-value");
  EXPECT_STRNE((*conf).Get("custom-prop", "").c_str(), "some-value");
}

/*
 * Config will be loaded from hbase-defualt.xml and hbase-site.xml @
 * kDefHBaseConfPath and kHBaseConfPath respectively.
 */
TEST(Configuration, LoadConfFromMultipleLocatons) {
  // Remove already configured env if present.
  unsetenv("HBASE_CONF");
  CreateHBaseConf(kDefHBaseConfPath, kHBaseDefaultXml, kHBaseDefaultXmlData);
  CreateHBaseConf(kDefHBaseConfPath, kHBaseSiteXml, kHBaseSiteXmlData);
  CreateHBaseConf(kHBaseConfPath, kHBaseDefaultXml, kHBaseDefaultXmlData);
  CreateHBaseConf(kHBaseConfPath, kHBaseSiteXml, kHBaseSiteXmlData);

  HBaseConfigurationLoader loader;
  std::string conf_paths = kDefHBaseConfPath + ":" + kHBaseConfPath;
  std::vector<std::string> resources{kHBaseDefaultXml, kHBaseSiteXml};
  hbase::optional<Configuration> conf =
      loader.LoadResources(conf_paths, resources);
  ASSERT_TRUE(conf) << "No configuration object present.";
  EXPECT_STREQ((*conf).Get("default-prop", "From hbase-default.xml").c_str(),
               "default-value");
  EXPECT_STREQ((*conf).Get("custom-prop", "").c_str(), "custom-value");
  EXPECT_STRNE((*conf).Get("custom-prop", "").c_str(), "some-value");
}

/*
 * Config will be loaded from hbase-defualt.xml and hbase-site.xml @
 * $HBASE_CONF.
 * We set HBASE_CONF to kDefHBaseConfPath
 * Below tests load the conf files in the same way unless specified.
 */
TEST(Configuration, DefaultValues) {
  // Remove already configured env if present.
  unsetenv("HBASE_CONF");
  CreateHBaseConfWithEnv();

  HBaseConfigurationLoader loader;
  hbase::optional<Configuration> conf = loader.LoadDefaultResources();
  ASSERT_TRUE(conf) << "No configuration object present.";
  EXPECT_STREQ((*conf).Get("default-prop", "Set this value.").c_str(),
               "default-value");
  EXPECT_STREQ((*conf).Get("custom-prop", "Set this value.").c_str(),
               "custom-value");
}

TEST(Configuration, FinalValues) {
  // Remove already configured env if present.
  unsetenv("HBASE_CONF");
  CreateHBaseConfWithEnv();

  HBaseConfigurationLoader loader;
  hbase::optional<Configuration> conf = loader.LoadDefaultResources();
  ASSERT_TRUE(conf) << "No configuration object present.";
  EXPECT_STREQ((*conf).Get("hbase.rootdir", "").c_str(),
               "/root/hbase-docker/apps/hbase/data");
  EXPECT_STREQ((*conf).Get("hbase.zookeeper.property.datadir", "").c_str(),
               "/root/hbase-docker/zookeeper");
  EXPECT_STRNE((*conf).Get("hbase.rootdir", "").c_str(),
               "This value will not be be overwritten");
  EXPECT_STRNE((*conf).Get("hbase.zookeeper.property.datadir", "").c_str(),
               "This value will be overwritten");
}

/*
 * Config will be loaded from HBASE_CONF which we set in
 * CreateHBaseConfWithEnv().
 * Config values will be loaded from hbase-default.xml and hbase-site.xml in the
 * above path.
 */
TEST(Configuration, EnvVars) {
  // Remove already configured env if present.
  unsetenv("HBASE_CONF");
  CreateHBaseConfWithEnv();

  HBaseConfigurationLoader loader;
  hbase::optional<Configuration> conf = loader.LoadDefaultResources();
  ASSERT_TRUE(conf) << "No configuration object present.";
  EXPECT_STREQ((*conf).Get("hbase-client.user.name", "").c_str(),
               "${user.name}");
  EXPECT_STRNE((*conf).Get("hbase-client.user.name", "root").c_str(),
               "test-user");
}

TEST(Configuration, SelfRef) {
  // Remove already configured env if present.
  unsetenv("HBASE_CONF");
  CreateHBaseConfWithEnv();

  HBaseConfigurationLoader loader;
  hbase::optional<Configuration> conf = loader.LoadDefaultResources();
  ASSERT_TRUE(conf) << "No configuration object present.";
  EXPECT_STREQ((*conf).Get("selfRef", "${selfRef}").c_str(), "${selfRef}");
}

TEST(Configuration, VarExpansion) {
  // Remove already configured env if present.
  unsetenv("HBASE_CONF");
  CreateHBaseConfWithEnv();

  HBaseConfigurationLoader loader;
  hbase::optional<Configuration> conf = loader.LoadDefaultResources();
  ASSERT_TRUE(conf) << "No configuration object present.";
  EXPECT_STREQ((*conf).Get("foo.substs", "foo-value").c_str(),
               "bar-value,bar-value,bar-value,bar-value,bar-value,bar-value,"
               "bar-value,bar-value,bar-value,bar-value,");
  EXPECT_STRNE((*conf).Get("foo.substs", "foo-value").c_str(), "bar-value");
}

TEST(Configuration, VarExpansionException) {
  // Remove already configured env if present.
  unsetenv("HBASE_CONF");
  CreateHBaseConfWithEnv();

  HBaseConfigurationLoader loader;
  hbase::optional<Configuration> conf = loader.LoadDefaultResources();
  ASSERT_TRUE(conf) << "No configuration object present.";
  ASSERT_THROW((*conf).Get("foo.substs.exception", "foo-value").c_str(),
               std::runtime_error);
}

TEST(Configuration, GetInt) {
  // Remove already configured env if present.
  unsetenv("HBASE_CONF");
  CreateHBaseConfWithEnv();

  HBaseConfigurationLoader loader;
  hbase::optional<Configuration> conf = loader.LoadDefaultResources();
  ASSERT_TRUE(conf) << "No configuration object present.";
  EXPECT_EQ(16000, (*conf).GetInt("int", 0));
  EXPECT_EQ(2147483646, (*conf).GetInt("int.largevalue", 0));
}

TEST(Configuration, GetLong) {
  // Remove already configured env if present.
  unsetenv("HBASE_CONF");
  CreateHBaseConfWithEnv();

  HBaseConfigurationLoader loader;
  hbase::optional<Configuration> conf = loader.LoadDefaultResources();
  ASSERT_TRUE(conf) << "No configuration object present.";
  EXPECT_EQ(2147483850, (*conf).GetLong("long", 0));
  EXPECT_EQ(9223372036854775807, (*conf).GetLong("long.largevalue", 0));
}

TEST(Configuration, GetDouble) {
  // Remove already configured env if present.
  unsetenv("HBASE_CONF");
  CreateHBaseConfWithEnv();

  HBaseConfigurationLoader loader;
  hbase::optional<Configuration> conf = loader.LoadDefaultResources();
  ASSERT_TRUE(conf) << "No configuration object present.";
  EXPECT_DOUBLE_EQ(17.9769e+100, (*conf).GetDouble("double", 0.0));
  EXPECT_DOUBLE_EQ(170.769e+200, (*conf).GetDouble("double.largevalue", 0.0));
}

TEST(Configuration, GetBool) {
  // Remove already configured env if present.
  unsetenv("HBASE_CONF");
  CreateHBaseConfWithEnv();

  HBaseConfigurationLoader loader;
  hbase::optional<Configuration> conf = loader.LoadDefaultResources();
  ASSERT_TRUE(conf) << "No configuration object present.";
  EXPECT_EQ(true, (*conf).GetBool("bool.true", true));
  EXPECT_EQ(false, (*conf).GetBool("bool.false", false));
}

TEST(Configuration, GetIntException) {
  // Remove already configured env if present.
  unsetenv("HBASE_CONF");
  CreateHBaseConfWithEnv();

  HBaseConfigurationLoader loader;
  hbase::optional<Configuration> conf = loader.LoadDefaultResources();
  ASSERT_TRUE(conf) << "No configuration object present.";
  ASSERT_THROW((*conf).GetInt("int.exception", 0), std::runtime_error);
}

TEST(Configuration, GetLongException) {
  // Remove already configured env if present.
  unsetenv("HBASE_CONF");
  CreateHBaseConfWithEnv();

  HBaseConfigurationLoader loader;
  hbase::optional<Configuration> conf = loader.LoadDefaultResources();
  ASSERT_TRUE(conf) << "No configuration object present.";
  ASSERT_THROW((*conf).GetLong("long.exception", 0), std::runtime_error);
}

TEST(Configuration, GetDoubleException) {
  // Remove already configured env if present.
  unsetenv("HBASE_CONF");
  CreateHBaseConfWithEnv();

  HBaseConfigurationLoader loader;
  hbase::optional<Configuration> conf = loader.LoadDefaultResources();
  ASSERT_TRUE(conf) << "No configuration object present.";
  ASSERT_THROW((*conf).GetDouble("double.exception", 0), std::runtime_error);
}

TEST(Configuration, GetBoolException) {
  // Remove already configured env if present.
  unsetenv("HBASE_CONF");
  CreateHBaseConfWithEnv();

  HBaseConfigurationLoader loader;
  hbase::optional<Configuration> conf = loader.LoadDefaultResources();
  ASSERT_TRUE(conf) << "No configuration object present.";
  ASSERT_THROW((*conf).GetBool("bool.exception", false), std::runtime_error);
}
