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

#include "test-util/mini-cluster.h"
#include <glog/logging.h>
#include <fcntl.h>
#include <boost/filesystem/operations.hpp>
#include <boost/filesystem/fstream.hpp>
#include <fstream>


using hbase::MiniCluster;

JNIEnv *MiniCluster::CreateVM(JavaVM **jvm) {
  JavaVMInitArgs args;
  JavaVMOption jvm_options;
  args.version = JNI_VERSION_1_6;
  args.nOptions = 1;
  char *classpath = getenv("CLASSPATH");
  std::string clspath;
  if (classpath == NULL || strstr(classpath, "-tests.jar") == NULL) {
    std::string clsPathFilePath("../target/cached_classpath.txt");
    std::ifstream fd(clsPathFilePath);
    std::string prefix("");
    if (fd.is_open()) {
      if (classpath == NULL) {
        LOG(INFO) << "got empty classpath";
      } else {
        // prefix bootstrapper.jar
        prefix.assign(classpath);
      }
      std::string line;
      if (getline(fd, line)) {
        clspath = prefix + ":" + line;
        int ret = setenv("CLASSPATH", clspath.c_str(), 1);
        LOG(INFO) << "set clspath " << ret;
      } else {
        LOG(INFO) << "nothing read from " << clsPathFilePath;
        exit(-1);
      }
    } else {
      LOG(INFO) << "nothing read from " << clsPathFilePath;
      exit(-1);
    }
    fd.close();
  }
  auto options = std::string {"-Djava.class.path="} + clspath;
  jvm_options.optionString = const_cast<char *>(options.c_str());
  args.options = &jvm_options;
  args.ignoreUnrecognized = 0;
  int rv;
  rv = JNI_CreateJavaVM(jvm, reinterpret_cast<void **>(&env_), &args);
  if (rv < 0 || !env_) {
    LOG(INFO) << "Unable to Launch JVM " << rv;
  } else {
    LOG(INFO) << "Launched JVM! " << options;
  }
  return env_;
}

void MiniCluster::WriteConf(jobject conf, const std::string& filepath) {
  jclass class_fdesc = env_->FindClass("java/io/FileDescriptor");
  // construct a new FileDescriptor
  jmethodID const_fdesc = env_->GetMethodID(class_fdesc, "<init>", "()V");

  jobject file = env_->NewObject(class_fdesc, const_fdesc);
  jfieldID field_fd = env_->GetFieldID(class_fdesc, "fd", "I");

  int fd = open(filepath.c_str(), O_RDWR | O_NONBLOCK | O_CREAT, S_IRWXU);
  if (fd < 0) {
    LOG(INFO) << "Couldn't open file " << filepath.c_str();
    exit(-1);
  }
  env_->SetIntField(file, field_fd, fd);

  jclass cls_outstream = env_->FindClass("java/io/FileOutputStream");
  jmethodID ctor_stream = env_->GetMethodID(cls_outstream, "<init>", "(Ljava/io/FileDescriptor;)V");
  if (ctor_stream == NULL) {
    LOG(INFO) << "Couldn't get ctor for FileOutputStream";
    exit(-1);
  }
  jobject file_outstream = env_->NewObject(cls_outstream, ctor_stream, file);
  if (file_outstream == NULL) {
    LOG(INFO) << "Couldn't create FileOutputStream";
    exit(-1);
  }
  jmethodID writeXmlMid = env_->GetMethodID(conf_class_, "writeXml", "(Ljava/io/OutputStream;)V");
  env_->CallObjectMethod(conf, writeXmlMid, file_outstream);
}
void MiniCluster::Setup() {
  jmethodID constructor;
  pthread_mutex_lock(&count_mutex_);
  if (env_ == NULL) {
    env_ = CreateVM(&jvm);
    if (env_ == NULL) {
      exit(-1);
    }
    testing_util_class_ = env_->FindClass("org/apache/hadoop/hbase/HBaseTestingUtility");
    // this should be converted to a globalref I think to avoid the underlying java obj getting
    // GC'ed
    if (testing_util_class_ == NULL) {
      LOG(INFO) << "Couldn't find class HBaseTestingUtility";
      exit(-1);
    }
    jmethodID mid = env_->GetStaticMethodID(testing_util_class_, "createLocalHTU",
                                           "()Lorg/apache/hadoop/hbase/HBaseTestingUtility;");
    htu_ = env_->CallStaticObjectMethod(testing_util_class_, mid);
    // this should be converted to a globalref I think to avoid the underlying java obj getting
    // GC'ed
    if (htu_ == NULL) {
      LOG(INFO) << "Couldn't invoke method createLocalHTU in HBaseTestingUtility";
      exit(-1);
    }
    get_conn_mid_ = env_->GetMethodID(testing_util_class_, "getConnection",
                                  "()Lorg/apache/hadoop/hbase/client/Connection;");
    jclass connClass = env_->FindClass("org/apache/hadoop/hbase/client/Connection");
    get_admin_mid_ =
        env_->GetMethodID(connClass, "getAdmin", "()Lorg/apache/hadoop/hbase/client/Admin;");
    get_table_mid_ = env_->GetMethodID(
        connClass, "getTable",
        "(Lorg/apache/hadoop/hbase/TableName;)Lorg/apache/hadoop/hbase/client/Table;");
    if (get_table_mid_ == NULL) {
      LOG(INFO) << "Couldn't find getConnection";
      exit(-1);
    }
    jclass adminClass = env_->FindClass("org/apache/hadoop/hbase/client/Admin");
    move_mid_ = env_->GetMethodID(adminClass, "move", "([B[B)V");
    if (move_mid_ == NULL) {
      LOG(INFO) << "Couldn't find move";
      exit(-1);
    }
    create_table_mid_ = env_->GetMethodID(testing_util_class_, "createTable",
                                      "(Lorg/apache/hadoop/hbase/TableName;Ljava/lang/String;)Lorg/"
                                      "apache/hadoop/hbase/client/Table;");
    create_table_with_split_mid_ = env_->GetMethodID(
        testing_util_class_, "createTable",
        "(Lorg/apache/hadoop/hbase/TableName;[[B[[B)Lorg/apache/hadoop/hbase/client/Table;");
    if (create_table_with_split_mid_ == NULL) {
      LOG(INFO) << "Couldn't find method createTable with split";
      exit(-1);
    }

    table_name_class_ = env_->FindClass("org/apache/hadoop/hbase/TableName");
    tbl_name_value_of_mid_ = env_->GetStaticMethodID(
        table_name_class_, "valueOf", "(Ljava/lang/String;)Lorg/apache/hadoop/hbase/TableName;");
    if (tbl_name_value_of_mid_ == NULL) {
      LOG(INFO) << "Couldn't find method valueOf in TableName";
      exit(-1);
    }
    jclass hbaseMiniClusterClass = env_->FindClass("org/apache/hadoop/hbase/MiniHBaseCluster");
    stop_rs_mid_ =
        env_->GetMethodID(hbaseMiniClusterClass, "stopRegionServer",
                         "(I)Lorg/apache/hadoop/hbase/util/JVMClusterUtil$RegionServerThread;");
    get_conf_mid_ = env_->GetMethodID(hbaseMiniClusterClass, "getConfiguration",
                                  "()Lorg/apache/hadoop/conf/Configuration;");

    conf_class_ = env_->FindClass("org/apache/hadoop/conf/Configuration");
    set_conf_mid_ = env_->GetMethodID(conf_class_, "set", "(Ljava/lang/String;Ljava/lang/String;)V");
    if (set_conf_mid_ == NULL) {
      LOG(INFO) << "Couldn't find method getConf in MiniHBaseCluster";
      exit(-1);
    }
    conf_get_mid_ = env_->GetMethodID(conf_class_, "get", "(Ljava/lang/String;)Ljava/lang/String;");

    jclass tableClass = env_->FindClass("org/apache/hadoop/hbase/client/Table");
    put_mid_ = env_->GetMethodID(tableClass, "put", "(Lorg/apache/hadoop/hbase/client/Put;)V");
    jclass connFactoryClass = env_->FindClass("org/apache/hadoop/hbase/client/ConnectionFactory");
    create_conn_mid_ = env_->GetStaticMethodID(connFactoryClass, "createConnection",
                                           "()Lorg/apache/hadoop/hbase/client/Connection;");
    if (create_conn_mid_ == NULL) {
      LOG(INFO) << "Couldn't find createConnection";
      exit(-1);
    }
    put_class_ = env_->FindClass("org/apache/hadoop/hbase/client/Put");
    put_ctor_ = env_->GetMethodID(put_class_, "<init>", "([B)V");
    add_col_mid_ =
        env_->GetMethodID(put_class_, "addColumn", "([B[B[B)Lorg/apache/hadoop/hbase/client/Put;");
    if (add_col_mid_ == NULL) {
      LOG(INFO) << "Couldn't find method addColumn";
      exit(-1);
    }
  }
  pthread_mutex_unlock(&count_mutex_);
}

jobject MiniCluster::htu() {
  Setup();
  return htu_;
}

JNIEnv *MiniCluster::env() {
  Setup();
  return env_;
}
// converts C char* to Java byte[]
jbyteArray MiniCluster::StrToByteChar(const std::string& str) {
  char *p = const_cast<char*>(str.c_str());
  int n = 0;
  while (*p++) {
    n++;
  }
  if (n == NULL) return NULL;
  jbyteArray arr = env_->NewByteArray(n);
  env_->SetByteArrayRegion(arr, 0, n, reinterpret_cast<const jbyte *>(str.c_str()));
  return arr;
}

jobject MiniCluster::CreateTable(std::string tblNam, std::string familyName) {
  jstring tblNameStr = env_->NewStringUTF(tblNam.c_str());
  jobject tblName = env_->CallStaticObjectMethod(table_name_class_, tbl_name_value_of_mid_, tblNameStr);
  jstring famStr = env_->NewStringUTF(familyName.c_str());
  jobject tbl = env_->CallObjectMethod(htu_, create_table_mid_, tblName, famStr);
  return tbl;
}
jobject MiniCluster::CreateTable(std::string tblNam, std::string familyName, std::string key1,
        std::string key2) {
  jstring tblNameStr = env_->NewStringUTF(tblNam.c_str());
  jobject tblName = env_->CallStaticObjectMethod(table_name_class_, tbl_name_value_of_mid_, tblNameStr);
  jclass arrayElemType = env_->FindClass("[B");

  jobjectArray famArray = env_->NewObjectArray(1, arrayElemType, env_->NewByteArray(1));
  env_->SetObjectArrayElement(famArray, 0, StrToByteChar(familyName));

  int len = 2;
  if (key2.empty()) len = 1;
  jobjectArray keyArray = env_->NewObjectArray(len, arrayElemType, env_->NewByteArray(1));

  env_->SetObjectArrayElement(keyArray, 0, StrToByteChar(key1));
  if (!key2.empty()) {
    env_->SetObjectArrayElement(keyArray, 1, StrToByteChar(key2));
  }
  jobject tbl = env_->CallObjectMethod(htu_, create_table_with_split_mid_, tblName, famArray, keyArray);
  return tbl;
}

jobject MiniCluster::StopRegionServer(jobject cluster, int idx) {
  env();
  return env_->CallObjectMethod(cluster, stop_rs_mid_, (jint)idx);
}

// returns the Configuration for the cluster
jobject MiniCluster::GetConf() {
  env();
  return env_->CallObjectMethod(cluster_, get_conf_mid_);
}
// return the Admin instance for the local cluster
jobject MiniCluster::admin() {
  env();
  jobject conn = env_->CallObjectMethod(htu(), get_conn_mid_);
  jobject admin = env_->CallObjectMethod(conn, get_admin_mid_);
  return admin;
}

jobject MiniCluster::TablePut(const std::string table, const std::string row, const std::string fam,
  const std::string col, const std::string value) {
  env();
  jobject conn = env_->CallObjectMethod(htu(), get_conn_mid_);
  jobject put = env_->NewObject(put_class_, put_ctor_, StrToByteChar(row));
  if (put == NULL) {
    LOG(INFO) << "Couldn't create Put";
    exit(-1);
  }
  env_->CallObjectMethod(put, add_col_mid_, StrToByteChar(fam), StrToByteChar(col),
                        StrToByteChar(value));
  jobject tblName =
      env_->CallStaticObjectMethod(table_name_class_, tbl_name_value_of_mid_,
              env_->NewStringUTF(table.c_str()));
  jobject tableObj = env_->CallObjectMethod(conn, get_table_mid_, tblName);
  env_->CallObjectMethod(tableObj, put_mid_, put);
  return tableObj;
}

// moves region to server
void MiniCluster::MoveRegion(std::string region, std::string server) {
  jobject admin_ = admin();
  env_->CallObjectMethod(admin_, move_mid_, StrToByteChar(region),
          StrToByteChar(server));
}

jobject MiniCluster::StartCluster(int numRegionServers, std::string conf_path) {
  env();
  jmethodID mid = env_->GetMethodID(testing_util_class_, "startMiniCluster",
                                   "(I)Lorg/apache/hadoop/hbase/MiniHBaseCluster;");
  if (mid == NULL) {
    LOG(INFO) << "Couldn't find method startMiniCluster in the class HBaseTestingUtility";
    exit(-1);
  }
  cluster_ = env_->CallObjectMethod(htu(), mid, (jint)numRegionServers);
  jobject conf = GetConf();
  jstring jport = (jstring)env_->CallObjectMethod(
      conf, conf_get_mid_, env_->NewStringUTF("hbase.zookeeper.property.clientPort"));
  const char *port = env_->GetStringUTFChars(jport, 0);
  LOG(INFO) << "retrieved port " << port;
  std::string quorum("localhost:");
  env_->CallObjectMethod(conf, set_conf_mid_, env_->NewStringUTF("hbase.zookeeper.quorum"),
                        env_->NewStringUTF((quorum + port).c_str()));
  if (!conf_path.empty()) {
    // Directory will be created if not present
    if (!boost::filesystem::exists(conf_path)) {
      boost::filesystem::create_directories(conf_path);
    }
    WriteConf(conf, conf_path + "/hbase-site.xml");
  }
  return cluster_;
}

void MiniCluster::StopCluster() {
  env();
  jmethodID mid = env_->GetMethodID(testing_util_class_, "shutdownMiniCluster", "()V");
  env_->CallVoidMethod(htu(), mid);
  if (jvm != NULL) {
     jvm->DestroyJavaVM();
     jvm = NULL;
  }
}

const std::string MiniCluster::GetConfValue(std::string key) {
  jobject conf = GetConf();
  jstring jval = (jstring)env_->CallObjectMethod(conf, conf_get_mid_,
          env_->NewStringUTF(key.c_str()));
  const char *val = env_->GetStringUTFChars(jval, 0);
  return val;
}
