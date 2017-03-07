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

#include <string>
#include "jni.h"

namespace hbase {

class MiniCluster {
 public:
  jobject StartCluster(int numRegionServers, std::string conf_path);
  void StopCluster();
  jobject CreateTable(std::string tblNam, std::string familyName);
  jobject CreateTable(std::string tblNam, std::string familyName, std::string key1, std::string k2);
  jobject StopRegionServer(jobject cluster, int idx);

  // moves region to server
  void MoveRegion(std::string region, std::string server);
  // returns the Configuration instance for the cluster
  jobject GetConf();
  // returns the value for config key retrieved from cluster
  const std::string GetConfValue(std::string key);
  // Does Put into table for family fam, qualifier col with value
  jobject TablePut(const std::string table, const std::string row, const std::string fam,
    const std::string col, const std::string value);

 private:
  JNIEnv *env_;
  jclass testing_util_class_;
  jclass table_name_class_;
  jclass put_class_;
  jclass conf_class_;
  jmethodID stop_rs_mid_;
  jmethodID get_conf_mid_;
  jmethodID set_conf_mid_;
  jmethodID tbl_name_value_of_mid_;
  jmethodID create_table_mid_;
  jmethodID create_table_with_split_mid_;
  jmethodID put_mid_;
  jmethodID put_ctor_;
  jmethodID add_col_mid_;
  jmethodID create_conn_mid_;
  jmethodID get_conn_mid_;
  jmethodID get_table_mid_;
  jmethodID conf_get_mid_;
  jmethodID get_admin_mid_;
  jmethodID move_mid_;
  jmethodID str_ctor_mid_;
  jobject htu_;
  jobject cluster_;
  pthread_mutex_t count_mutex_;
  JavaVM *jvm;
  JNIEnv *CreateVM(JavaVM **jvm);
  void WriteConf(jobject conf, const std::string& filepath);
  void Setup();
  jobject htu();
  JNIEnv *env();
  jbyteArray StrToByteChar(const std::string& str);
  jobject admin();
};
} /*namespace hbase*/
