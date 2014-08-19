/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
#ifndef HBASE_JNI_IMPL_JNIHELPER_H__
#define HBASE_JNI_IMPL_JNIHELPER_H__

#include <jni.h>
#include <stdint.h>

#include <hbase/log.h>
#include <hbase/types.h>

#include "hbase_status.h"

#line __LINE__ "jnihelper.h"

/* Macros for constructing method signatures */
#define JPARAM(X)               "L" X ";"
#define JARRPARAM(X)            "[L" X ";"
#define JMETHOD1(X, R)          "(" X ")" R
#define JMETHOD2(X, Y, R)       "(" X Y ")" R
#define JMETHOD3(X, Y, Z, R)    "(" X Y Z")" R

#define HADOOP_CONF             "org/apache/hadoop/conf/Configuration"
#define HBASE_CONF              "org/apache/hadoop/hbase/HBaseConfiguration"
#define HBASE_GET               "org/apache/hadoop/hbase/client/Get"
#define HBASE_PUT               "org/apache/hadoop/hbase/client/Put"
#define HBASE_MUTATION          "org/apache/hadoop/hbase/client/Mutation"
#define HBASE_RESULT            "org/apache/hadoop/hbase/client/Result"
#define HBASE_HTABLE            "org/apache/hadoop/hbase/client/HTable"
#define HBASE_HADMIN            "org/apache/hadoop/hbase/client/HBaseAdmin"
#define HBASE_TBLDSC            "org/apache/hadoop/hbase/HTableDescriptor"
#define HBASE_CLMDSC            "org/apache/hadoop/hbase/HColumnDescriptor"
#define HBASE_DURABILITY        "org/apache/hadoop/hbase/client/Durability"

#define CLASS_CLIENT_PROXY      "org/apache/hadoop/hbase/jni/ClientProxy"
#define CLASS_GET_PROXY         "org/apache/hadoop/hbase/jni/GetProxy"
#define CLASS_PUT_PROXY         "org/apache/hadoop/hbase/jni/PutProxy"
#define CLASS_ROW_PROXY         "org/apache/hadoop/hbase/jni/RowProxy"
#define CLASS_RESULT_PROXY      "org/apache/hadoop/hbase/jni/ResultProxy"
#define CLASS_DELETE_PROXY      "org/apache/hadoop/hbase/jni/DeleteProxy"
#define CLASS_MUTATION_PROXY    "org/apache/hadoop/hbase/jni/MutationProxy"
#define CLASS_SCANNER_PROXY     "org/apache/hadoop/hbase/jni/ScannerProxy"
#define CLASS_TABLE_PROXY       "org/apache/hadoop/hbase/jni/TableProxy"

#define ASYNC_REMOTEEXCEPTION   "org/hbase/async/RemoteException"

#define JAVA_OBJECT             "java/lang/Object"
#define JAVA_THROWABLE          "java/lang/Throwable"
#define JAVA_STACKTRACEELM      "java/lang/StackTraceElement"
#define JAVA_STRING             "java/lang/String"
#define JAVA_LIST               "java/util/List"
#define JAVA_ARRAYLIST          "java/util/ArrayList"

/**
 * Retrieves a JNIEnv* unless one was provided.
 * Pushes a JNI local frame on the stack.
 */
#define JNI_GET_ENV(current_env) \
  JNIEnv *env = current_env; \
  if (env == NULL) { \
    env = JniHelper::GetJNIEnv(); \
    if (env == NULL) return Status::HBaseInternalError; \
  } \
  JniLocalFrame jni_frame(env); \
  RETURN_IF_ERROR(jni_frame.push());

namespace hbase {

class JniLocalFrame {
 public:
  JniLocalFrame(JNIEnv *env) : env_(env) {}
  ~JniLocalFrame() {
    if (env_ != NULL) {
      env_->PopLocalFrame(NULL);
    }
  }

  Status push(int max_local_ref=10) {
    int err = 0;
    if (env_ == NULL) {
      HBASE_LOG_ERROR("Can not push JNI local frame since JNIEnv* is NULL.");
      return Status::EInvalid;
    } else if ((err = env_->PushLocalFrame(max_local_ref)) < 0) {
      HBASE_LOG_ERROR("Error %d pushing JNI local frame ", err);
      env_->ExceptionClear();
      return Status::HBaseInternalError;
    }
    return Status::Success;
  }

 private:
  JNIEnv *env_;
};

class JniResult : public Status {
public:
  JniResult(int32_t code=0, const char *msg="");

  jvalue GetValue() { return value; }

  jobject GetObject() { return value.l; }

  friend class JniHelper;

private:
  jvalue        value;
};

/**
 * All HBase object wrapping a JAVA proxy object derives from JniObject.
 */
class JniObject {
public:
  JniObject() : jobject_(NULL) {}

  JniObject(jobject object) : jobject_(object) {}

  virtual ~JniObject();

  jobject JObject() const { return jobject_; }

protected:
  jobject jobject_;

private:
  Status Destroy(JNIEnv *current_env=NULL);
};

class JniHelper {
public:

  /**
   * GetJNIEnv: A helper function to get the JNIEnv* for the given thread.
   * If no JVM exists, then one will be created. JVM command line arguments
   * are obtained from the LIBHDFS_OPTS environment variable.
   *
   * @param: None.
   * @returns The JNIEnv* corresponding to the thread.
   */
  static JNIEnv *GetJNIEnv(void);

  /**
   * Invokes a constructor.
   *
   * env: The JNIEnv pointer
   * className: Name of the class
   * ctorSignature: the signature of the constructor "(arg-types)V"
   *
   * Arguments to the constructor must be passed after ctorSignature
   */
  static JniResult NewObject(
      JNIEnv *env,
      const char *className,
      const char *ctorSignature,
      ...);  /* the constructor arguments */

  /**
   * Invokes an Instance method.
   *
   * RETURNS: a JniResult object
   */
  static JniResult InvokeMethod(
      JNIEnv *env,            /* The JNIEnv pointer */
      jobject instObj,        /* The object to invoke the method on. */
      const char *className,  /* Name of the class where the method can be found */
      const char *methName,   /* Name of the method */
      const char *methSignature,  /* the signature of the method "(arg-types)ret-type" */
      ...);  /* the method arguments */

  /**
   * Invokes an Static method.
   *
   * RETURNS: a JniResult object
   */
  static JniResult InvokeMethodS(
      JNIEnv *env,           /* The JNIEnv pointer */
      const char *className, /* Name of the class where the method can be found */
      const char *methName,      /* Name of the method */
      const char *methSignature, /* the signature of the method "(arg-types)ret-type" */
      ...);  /* the method arguments */

  /**
   * Parse the Java exception, optionally logging it and returns the
   * unique code identifying the exception.
   *
   * @returns The unique code of the exception or 0 if the exception was null.
   */
  static int32_t ParseJavaException(
      JNIEnv *env,      /* The JNIEnv pointer */
      jthrowable jthr,  /* The Java exception to translate */
      bool logException = true);

  /**
   * Translates an exception into errno.
   *
   * @returns The translated error code or 0 if the exception was null.
   */
  static int32_t ErrorFromException(
      JNIEnv *env,      /* The JNIEnv pointer */
      jthrowable jthr);  /* The Java exception to translate */

  /**
   * Convert a C/C++ char array to Java byte array
   *
   * @returns A JniResult referring to the created byte array.
   */
  static JniResult CreateJavaByteArray(
      JNIEnv *env,
      const byte_t *buf,
      const jsize start,
      const jsize len);

  static Status CreateByteArray(
      JNIEnv *env,
      jobject jbyteArray,
      byte_t **bufPtr,
      size_t *bufLen);

  static JniResult CreateJniObjectList(
      JNIEnv *env,
      JniObject **jniObjects,
      size_t numObjects);

  static const Status SetField(
      JNIEnv *env,
      const char *className,
      const char *fieldName,
      const char *fieldSignature,
      jobject val,
      jobject obj = NULL);

private:
  static JniResult InvokeMethodInternal(
      JNIEnv *env,
      const char *className,
      const char *methName,
      const char *methSignature,
      va_list args,
      jobject instObj = NULL); /* static method if NULL, instance otherwise */
};

} /* namespace hbase */

#endif /* HBASE_JNI_IMPL_JNIHELPER_H__ */
