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
#line 19 "jnihelper.cc" // ensures short filename in logs.

#include <jni.h>

#include <ctype.h>
#include <dirent.h>
#include <pthread.h>
#include <search.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#ifdef __CYGWIN__
  #include <sys/cygwin.h>
#endif

#include <hbase/types.h>

#include "hbase_consts.h"
#include "hbase_macros.h"
#include "hbase_status.h"
#include "jnihelper.h"

namespace hbase {

#ifdef __CYGWIN__
  #define PATH_SEPARATOR ";"
  #define FILE_SEPARATOR "\\"
#else
  #define PATH_SEPARATOR ":"
  #define FILE_SEPARATOR "/"
#endif

#define JAVA_CLASSPATH "CLASSPATH"
#define HBASE_LIB_DIR  "HBASE_LIB_DIR"
#define HBASE_CONF_DIR "HBASE_CONF_DIR"

static pthread_mutex_t hbaseHashMutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t jvmMutex = PTHREAD_MUTEX_INITIALIZER;

static volatile int hashTableInited = 0;

#define LOCK_HASH_TABLE() pthread_mutex_lock(&hbaseHashMutex)
#define UNLOCK_HASH_TABLE() pthread_mutex_unlock(&hbaseHashMutex)
#define LOCK_JVM_MUTEX() pthread_mutex_lock(&jvmMutex)
#define UNLOCK_JVM_MUTEX() pthread_mutex_unlock(&jvmMutex)

/** The Native return types that methods could return */
#define JVOID         'V'
#define JOBJECT       'L'
#define JARRAYOBJECT  '['
#define JBOOLEAN      'Z'
#define JBYTE         'B'
#define JCHAR         'C'
#define JSHORT        'S'
#define JINT          'I'
#define JLONG         'J'
#define JFLOAT        'F'
#define JDOUBLE       'D'

/** Java string hash codes of the exception class name */
static const int32_t java_lang_OutOfMemoryError                                       = 0xbca8c7d6;
static const int32_t org_apache_hadoop_hbase_NotServingRegionException                = 0xf9e9577a;
static const int32_t org_apache_hadoop_hbase_RegionTooBusyException                   = 0x44f9bcbe;
static const int32_t org_apache_hadoop_hbase_TableExistsException                     = 0xed09f7ed;
static const int32_t org_apache_hadoop_hbase_TableNotDisabledException                = 0x1e30f246;
static const int32_t org_apache_hadoop_hbase_TableNotEnabledException                 = 0x725f5e9b;
static const int32_t org_apache_hadoop_hbase_TableNotFoundException                   = 0x99394aba;
static const int32_t org_apache_hadoop_hbase_UnknownScannerException                  = 0x6021c113;
static const int32_t org_apache_hadoop_hbase_client_RegionOfflineException            = 0x10d6f53b;
static const int32_t org_apache_hadoop_hbase_regionserver_NoSuchColumnFamilyException = 0x8669901e;
static const int32_t org_apache_hadoop_hbase_security_AccessDeniedException           = 0xf8e728ca;
static const int32_t org_hbase_async_ConnectionResetException                         = 0x76e397af;
static const int32_t org_hbase_async_MultiAction_MultiPutFailedException              = 0x8706944c;
static const int32_t org_hbase_async_NoSuchColumnFamilyException                      = 0x8ccb093c;
static const int32_t org_hbase_async_NotServingRegionException                        = 0xf9da3971;
static const int32_t org_hbase_async_PleaseThrottleException                          = 0x0f0c51ea;
static const int32_t org_hbase_async_RegionOfflineException                           = 0x33bcca71;
static const int32_t org_hbase_async_RemoteException                                  = 0xa7fd8618;
static const int32_t org_hbase_async_TableNotFoundException                           = 0x42618423;
static const int32_t org_hbase_async_UnknownScannerException                          = 0xdc00b4ca;

/** Denote the method we want to invoke as STATIC or INSTANCE */
typedef enum {
    STATIC,
    INSTANCE
} MemberType;

typedef enum {
    REF_LOCAL,
    REF_GLOBAL
} RefType;

JniResult::JniResult(int32_t code, const char *msg)
: Status(code, msg) {
}

/**
 * MAX_HASH_TABLE_ELEM: The maximum no. of entries in the hashtable.
 * It's set to 4096 to account for (classNames + No. of threads)
 */
static const int MAX_HASH_TABLE_ELEM = 4096;

static int
HashTableInit(void) {
  if (!hashTableInited) {
    LOCK_HASH_TABLE();
    if (!hashTableInited) {
      if (hcreate(MAX_HASH_TABLE_ELEM) == 0) {
        HBASE_LOG_ERROR("Error creating hashtable, <%d>: %s",
            errno, strerror(errno));
        UNLOCK_HASH_TABLE();
        return 0;
      }
      hashTableInited = 1;
    }
    UNLOCK_HASH_TABLE();
  }
  return 1;
}

static int
InsertEntryIntoTable(const char *key, void *data) {
  ENTRY e, *ep;
  if (key == NULL || data == NULL) {
    return 0;
  }
  if (! HashTableInit()) {
    return -1;
  }
  e.data = data;
  e.key = (char*)key;
  LOCK_HASH_TABLE();
  ep = hsearch(e, ENTER);
  UNLOCK_HASH_TABLE();
  if (UNLIKELY(ep == NULL)) {
    HBASE_LOG_ERROR("Warn adding key (%s) to hash table, <%d>: %s",
        key, errno, strerror(errno));
  }
  return 0;
}

static void*
SearchEntryFromTable(const char *key) {
  ENTRY e,*ep;
  if (key == NULL) {
    return NULL;
  }
  HashTableInit();
  e.key = (char*)key;
  LOCK_HASH_TABLE();
  ep = hsearch(e, FIND);
  UNLOCK_HASH_TABLE();
  if (ep != NULL) {
    return ep->data;
  }
  return NULL;
}


static int
JarFilesOnly(const struct dirent *entry) {
  const char *filename = entry->d_name;
  unsigned int filenameLen = strlen(filename);
  // filename length should be greater than strlen(".jar")
  if (filenameLen < 4)
    return 0;
  const char *suffixStart = filename+(filenameLen-strlen(".jar"));
  // filename should end with "-filters.so"
  if (strcmp(suffixStart, ".jar") != 0)
    return 0;
  // All Okay
  return 1;
}

#define PATH_BUFFER_SIZE    4096
static std::string
AdjustCygwinPath(std::string originalPath) {
#ifdef __CYGWIN__
  char buffer[PATH_BUFFER_SIZE];
  cygwin_conv_path (CCP_POSIX_TO_WIN_A, originalPath.data(), buffer, PATH_BUFFER_SIZE);
  return buffer;
#endif
  return originalPath;
}

static std::string
BuildHBaseClassPath(
    const std::string& hbaseConfDir,
    const std::string& hbaseLibDir,
    const std::string& jvmClassPath) {
  std::string hbaseClassPath = "-Djava.class.path=";

  if (!hbaseConfDir.empty()) {
    char *absConfDir = realpath(hbaseConfDir.c_str(), NULL);
    if (UNLIKELY(absConfDir == NULL)) {
      HBASE_LOG_ERROR("Error accessing " HBASE_CONF_DIR ": '%s'. %s",
          hbaseConfDir.c_str(), strerror(errno));
      return HConstants::EMPTY_STRING;
    }

    hbaseClassPath.append(AdjustCygwinPath(absConfDir));
    free(absConfDir);
  }

  if (!hbaseLibDir.empty()) {
    char *absLibPath = realpath(hbaseLibDir.c_str(), NULL);
    if (UNLIKELY(absLibPath == NULL)) {
      HBASE_LOG_ERROR("Error accessing " HBASE_LIB_DIR " '%s'. %s",
          hbaseLibDir.c_str(), strerror(errno));
      return HConstants::EMPTY_STRING;
    }

    int jarCount = 0;
    struct dirent **jarList;
    // scan all the files matching glob "*.jar"
    if ((jarCount = scandir(absLibPath, &jarList, JarFilesOnly, alphasort)) > 0) {
      std::string dirPath = AdjustCygwinPath(absLibPath);
      for (int i = 0; i < jarCount; ++i) {
        hbaseClassPath.append(PATH_SEPARATOR)
            .append(dirPath)
            .append(FILE_SEPARATOR)
            .append(jarList[i]->d_name);
        free(jarList[i]);
      }
      free(jarList);
      free(absLibPath);
    }
  }

  if (!jvmClassPath.empty()) {
    hbaseClassPath.append(PATH_SEPARATOR)
        .append(jvmClassPath);
  }

  HBASE_LOG_DEBUG("Java classpath argument: %s", hbaseClassPath.c_str());

  return hbaseClassPath;
}

/**
 * Helper function to get the JNI class reference for the requested class.
 * This class caches the references and hence is faster than calling the JNI
 * method every time.
 */
static jclass
GetClassReference(
    JNIEnv *env,
    const char *className) {
  jclass clsLocalRef;
  jclass cls = (jclass) SearchEntryFromTable(className);
  if (cls) {
    return cls;
  }

  clsLocalRef = env->FindClass(className);
  if (UNLIKELY(clsLocalRef == NULL)) {
    HBASE_LOG_ERROR("Unable to get class id for class '%s'", className);
    env->ExceptionDescribe();
    env->ExceptionClear();
    return NULL;
  }
  cls = static_cast<jclass>(env->NewGlobalRef(clsLocalRef));
  if (UNLIKELY(cls == NULL)) {
    HBASE_LOG_ERROR("Unable to create a reference for class '%s'", className);
    env->ExceptionDescribe();
    env->ExceptionClear();
    return NULL;
  }
  env->DeleteLocalRef(clsLocalRef);
  InsertEntryIntoTable(className, cls);
  return cls;
}

static jmethodID
GetClassMethodId(
    JNIEnv *env,
    jclass cls,
    const char *className,
    const char *methName,
    const char *methSignature,
    MemberType methType) {
  jmethodID mid = 0;
  if (methType == STATIC) {
    mid = env->GetStaticMethodID(cls, methName, methSignature);
  }
  else if (methType == INSTANCE) {
    mid = env->GetMethodID(cls, methName, methSignature);
  }
  if (UNLIKELY(mid == NULL)) {
    HBASE_LOG_ERROR(
        "Could not find method %s from class %s with signature %s.",
        methName, className, methSignature);
  }
  return mid;
}

static std::string
GetClassNameOfObject(
    JNIEnv *env,
    jobject jobj) {
  jclass cls, clsClass;
  jmethodID mid;
  jstring str;

  cls = env->GetObjectClass(jobj);
  if (UNLIKELY(cls == NULL)) {
    HBASE_LOG_ERROR("env->GetObjectClass() failed");
    env->ExceptionDescribe();
    env->ExceptionClear();
    return HConstants::EMPTY_STRING;
  }
  clsClass = GetClassReference(env, "java/lang/Class");
  if (UNLIKELY(clsClass == NULL)) {
    return HConstants::EMPTY_STRING;
  }
  mid = env->GetMethodID(clsClass, "getName", "()Ljava/lang/String;");
  if (UNLIKELY(mid == NULL)) {
    HBASE_LOG_ERROR("env->GetMethodID() failed");
    env->ExceptionDescribe();
    env->ExceptionClear();
    return NULL;
  }
  str = (jstring)env->CallObjectMethod(cls, mid);
  if (UNLIKELY(str == NULL)) {
    HBASE_LOG_ERROR("env->CallObjectMethod() failed");
    env->ExceptionDescribe();
    env->ExceptionClear();
    return HConstants::EMPTY_STRING;
  }

  const char *cstr = env->GetStringUTFChars(str, NULL);
  if (UNLIKELY(cstr == NULL)) {
    HBASE_LOG_ERROR("env->GetStringUTFChars() failed");
    env->ExceptionDescribe();
    env->ExceptionClear();
    return HConstants::EMPTY_STRING;
  }

  std::string newstr(cstr);
  env->ReleaseStringUTFChars(str, cstr);
  return newstr;
}


static inline int32_t
JavaHashOfString(const char *buffer, size_t offset,
    size_t len) {
  int32_t hash = 0;
  size_t off = offset;
  for (size_t i = 0; i < len; ++off, ++i) {
    hash = 31*hash + buffer[off];
  }
  return hash;
}

int32_t
JniHelper::ParseJavaException(
    JNIEnv *env,
    jthrowable jthr,
    bool logException) {
  if (jthr == NULL) {
    return 0;
  }

  std::string excClass = GetClassNameOfObject(env, (jobject) jthr);
  int32_t excClassHash = JavaHashOfString(excClass.c_str(), 0, excClass.size());

  if (excClassHash == org_hbase_async_RemoteException) {
    // extract the wrapped exception
    static jmethodID getType = NULL;
    if (getType == NULL) {
      jclass cls = GetClassReference(env, ASYNC_REMOTEEXCEPTION);
      if (cls == NULL) {
        return HBASE_INTERNAL_ERR;
      }
      getType = GetClassMethodId(env, cls, ASYNC_REMOTEEXCEPTION,
          "getType", "()Ljava/lang/String;", INSTANCE);
      if (getType == NULL) {
        return HBASE_INTERNAL_ERR;
      }
    }
    jstring jExcType = (jstring) env->CallObjectMethod(jthr, getType);
    const char *execType = env->GetStringUTFChars(jExcType, NULL);
    excClassHash = JavaHashOfString(execType, 0, strlen(execType));
    env->ReleaseStringUTFChars(jExcType, execType);
  }

  bool throttleException = false;
  switch (excClassHash) {
  case org_hbase_async_PleaseThrottleException:
  case org_apache_hadoop_hbase_RegionTooBusyException:
    throttleException = true;
    break;
  }
  if (logException
      && (!throttleException
          || (hb_log_get_level() >= HBASE_LOG_LEVEL_TRACE))) {
    static jmethodID getMessage = NULL;
    if (getMessage == NULL) {
      jclass cls = GetClassReference(env, JAVA_THROWABLE);
      if (cls == NULL) {
        return HBASE_INTERNAL_ERR;
      }
      getMessage = GetClassMethodId(env, cls, JAVA_THROWABLE,
          "getMessage", "()L"JAVA_STRING";", INSTANCE);
      if (getMessage == NULL) {
        return HBASE_INTERNAL_ERR;
      }
    }
    jstring jMessage = (jstring) env->CallObjectMethod(jthr, getMessage);
    const char *message = env->GetStringUTFChars(jMessage, NULL);
    HBASE_LOG_ERROR("Java exception: %s\n\t%s", excClass.c_str(), message);
    env->ReleaseStringUTFChars(jMessage, message);
  }

  return excClassHash;
}

int32_t
JniHelper::ErrorFromException(
    JNIEnv *env,
    jthrowable jthr) {
  if (jthr == NULL) {
    return 0;
  }

  int32_t errnum = HBASE_INTERNAL_ERR;
  int32_t exceptionCode = ParseJavaException(env, jthr);

  switch(exceptionCode) {
  case java_lang_OutOfMemoryError:
    errnum = ENOMEM;
    break;
  case org_apache_hadoop_hbase_security_AccessDeniedException:
    errnum = EACCES;
    break;
  case org_apache_hadoop_hbase_TableNotFoundException:
  case org_apache_hadoop_hbase_regionserver_NoSuchColumnFamilyException:
  case org_hbase_async_TableNotFoundException:
  case org_hbase_async_NoSuchColumnFamilyException:
    errnum = ENOENT;
    break;
  case org_apache_hadoop_hbase_TableExistsException:
    errnum = EEXIST;
    break;
  case org_apache_hadoop_hbase_TableNotEnabledException:
    errnum = HBASE_TABLE_DISABLED;
    break;
  case org_apache_hadoop_hbase_TableNotDisabledException:
    errnum = HBASE_TABLE_NOT_DISABLED;
    break;
  case org_hbase_async_UnknownScannerException:
  case org_apache_hadoop_hbase_UnknownScannerException:
    errnum = HBASE_UNKNOWN_SCANNER;
    break;
  case org_apache_hadoop_hbase_NotServingRegionException:
  case org_hbase_async_NotServingRegionException:
  case org_apache_hadoop_hbase_client_RegionOfflineException:
  case org_hbase_async_RegionOfflineException:
  case org_hbase_async_MultiAction_MultiPutFailedException:
  case org_hbase_async_ConnectionResetException:
    errnum = EAGAIN;
    break;
  case org_hbase_async_PleaseThrottleException:
  case org_apache_hadoop_hbase_RegionTooBusyException:
    errnum = ENOBUFS;
    break;
  default:
    //TODO: interpret more exceptions
    break;
  }

  return errnum;
}

/**
 * This method has been intentionally placed below the other JNI helper
 * methods and none of the method above this should call it to avoid
 * infinite loop.
 */
static inline int32_t
CheckException(JNIEnv *env) {
  jthrowable jthr = env->ExceptionOccurred();
  if (LIKELY(jthr == NULL)) return 0;
  env->ExceptionClear();
  return JniHelper::ErrorFromException(env, jthr);
}

JniResult
JniHelper::InvokeMethodInternal(
    JNIEnv *env,
    const char *className,
    const char *methName,
    const char *methSignature,
    va_list args,
    jobject instObj) {
  MemberType methType = INSTANCE;
  if (instObj == NULL) {
    methType = STATIC;
  }
  jclass cls = GetClassReference(env, className);
  if (cls == NULL) {
    return JniResult(CheckException(env));
  }

  jmethodID method = GetClassMethodId(
      env, cls, className, methName, methSignature, methType);
  if (method == NULL) {
    return JniResult(CheckException(env));
  }

  JniResult result;
  const char *str = methSignature;
  while (*str != ')') str++;
  str++;
  switch(*str) {
  case JOBJECT:
  case JARRAYOBJECT:
    result.value.l = (methType == INSTANCE)
        ? env->CallObjectMethodV(instObj, method, args)
        : env->CallStaticObjectMethodV(cls, method, args);
    break;
  case JVOID:
    if (methType == INSTANCE) {
      env->CallVoidMethodV(instObj, method, args);
    } else {
      env->CallStaticVoidMethodV(cls, method, args);
    }
    break;
  case JBOOLEAN:
    result.value.z = (methType == INSTANCE)
        ? env->CallBooleanMethodV(instObj, method, args)
        : env->CallStaticBooleanMethodV(cls, method, args);
    break;
  case JSHORT:
    result.value.s = (methType == INSTANCE)
        ? env->CallShortMethodV(instObj, method, args)
        : env->CallStaticShortMethodV(cls, method, args);
    break;
  case JLONG:
    result.value.j = (methType == INSTANCE)
        ? env->CallLongMethodV(instObj, method, args)
        : env->CallStaticLongMethodV(cls, method, args);
    break;
  case JINT:
    result.value.i = (methType == INSTANCE)
        ? env->CallIntMethodV(instObj, method, args)
        : env->CallStaticIntMethodV(cls, method, args);
    break;
  }
  result.SetCode(CheckException(env));

  return result;
}

/**
 * In-place URL decoding
 */
static char*
urlDecode(char *orig) {
  char c1, c2;
  char *dest = orig, *src = orig;
  while (*src) {
    if ((*src == '%')
        && (c1 = *(src+1)) && isxdigit(c1)
        && (c2 = *(src+2)) && isxdigit(c2)) {
      c1 = tolower(c1);
      c1 -= (c1 >= 'a') ? ('a'-10) : '0';
      c2 = tolower(c2);
      c2 -= (c2 >= 'a') ? ('a'-10) : '0';
      *dest++ = (c1<<4)|c2;
      src += 3;
    } else {
      *dest++ = *src++;
    }
  }
  *dest = '\0';
  return orig;
}

static std::string
GetEnv(const char *name) {
  char *env_val = getenv(name);
  return (env_val == NULL) ? HConstants::EMPTY_STRING : env_val;
}

/** flag to record if the JVM was created by libhbase */
static volatile bool s_VMCreated = false;

JNIEnv*
JniHelper::GetJNIEnv(void) {
  // Only the first thread should create the JVM. The other threads should
  // just use the JVM created by the first thread.
  LOCK_JVM_MUTEX();

  jint noVMs = 0;
  const jsize vmBufLength = 1;
  JavaVM *vmBuf[vmBufLength];
  jint rv = JNI_GetCreatedJavaVMs(&(vmBuf[0]), vmBufLength, &noVMs);
  if (rv != 0) {
    HBASE_LOG_FATAL("JNI_GetCreatedJavaVMs failed with error: %d", rv);
    UNLOCK_JVM_MUTEX();
    return NULL;
  }

  JNIEnv *env = NULL;
  if (noVMs == 0) {
    //Get the environment variables for initializing the JVM
    std::string hbaseConfDir = GetEnv(HBASE_CONF_DIR);
    if (hbaseConfDir.empty()) {
      HBASE_LOG_WARN("Environment variable HBASE_CONF_DIR not set!");
    } else {
      HBASE_LOG_DEBUG("HBASE_CONF_DIR is set to [%s]", hbaseConfDir.c_str());
    }

    std::string hbaseClassPath = GetEnv(JAVA_CLASSPATH);
    std::string hbaseLibDir = GetEnv(HBASE_LIB_DIR);
    if (hbaseClassPath.empty() && hbaseLibDir.empty()) {
      HBASE_LOG_FATAL("At least one of the environment variables"
          " 'CLASSPATH' or 'HBASE_LIB_DIR' must be set!");
      UNLOCK_JVM_MUTEX();
      return NULL;
    }

    std::string optHBaseClassPath =
        BuildHBaseClassPath(hbaseConfDir, hbaseLibDir, hbaseClassPath);
    if (optHBaseClassPath.empty()) {
      UNLOCK_JVM_MUTEX();
      return NULL;
    }

    int noArgs = 1;
    //determine how many arguments were passed as LIBHBASE_OPTS env var
    char *hbaseJvmArgs = getenv("LIBHBASE_OPTS");
    char *copyOfhbaseJvmArgs = NULL;
    char jvmArgDelims[] = " ";
    if (hbaseJvmArgs != NULL) {
      HBASE_LOG_DEBUG("LIBHBASE_OPTS is set to [%s]", hbaseJvmArgs);
      char *result = NULL;
      copyOfhbaseJvmArgs = strdup(hbaseJvmArgs);
      result = strtok(copyOfhbaseJvmArgs, jvmArgDelims);
      while (result != NULL) {
        noArgs++;
        result = strtok(NULL, jvmArgDelims);
      }
    }
    JavaVMOption options[noArgs];
    options[0].optionString = (char*)optHBaseClassPath.c_str();
    //fill in any specified arguments
    if (noArgs > 1)  {
      char *result = NULL;
      strcpy(copyOfhbaseJvmArgs, hbaseJvmArgs);
      result = strtok(copyOfhbaseJvmArgs, jvmArgDelims);
      int argNum = 1;
      while(result != NULL) {
        options[argNum++].optionString = urlDecode(result);
        result = strtok(NULL, jvmArgDelims);
      }
    }

    fflush(stdout);
    //Create the VM
    JavaVMInitArgs vm_args;
    JavaVM *vm;
    vm_args.version = JNI_VERSION_1_2;
    vm_args.options = options;
    vm_args.nOptions = noArgs;
    vm_args.ignoreUnrecognized = 0;

    rv = JNI_CreateJavaVM(&vm, (void**)&env, (void*)&vm_args);
    if (rv != 0) {
      HBASE_LOG_FATAL("Call to JNI_CreateJavaVM failed "
          "with error: %d", rv);
      UNLOCK_JVM_MUTEX();
      return NULL;
    }
    s_VMCreated = true;
    free(copyOfhbaseJvmArgs);
  }
  else {
    if (!s_VMCreated) {
      HBASE_LOG_WARN("Found a JVM not created by libhbase.");
    }
    //Attach this thread to the VM
    JavaVM *vm = vmBuf[0];
    rv = vm->AttachCurrentThread((void**)&env, (void *)NULL);
    if (rv != 0) {
      HBASE_LOG_FATAL("Call to AttachCurrentThread failed with error: %d", rv);
      UNLOCK_JVM_MUTEX();
      return NULL;
    }
  }
  UNLOCK_JVM_MUTEX();

  return env;
}

JniResult
JniHelper::InvokeMethod(
    JNIEnv *env,
    jobject instObj,
    const char *className,
    const char *methName,
    const char *methSignature,
    ...) {
  va_list args;
  va_start(args, methSignature);
  JniResult result = InvokeMethodInternal(
      env, className, methName, methSignature, args, instObj);
  va_end(args);
  return result;
}

JniResult
JniHelper::InvokeMethodS(
    JNIEnv *env,
    const char *className,
    const char *methName,
    const char *methSignature,
    ...) {
  va_list args;
  va_start(args, methSignature);
  JniResult result = InvokeMethodInternal(
      env, className, methName, methSignature, args);
  va_end(args);
  return result;
}

JniResult
JniHelper::NewObject(
    JNIEnv *env,
    const char *className,
    const char *ctorSignature,
    ...) {
  jclass cls = GetClassReference(env, className);
  if (cls == NULL) {
    return JniResult(CheckException(env));
  }
  jmethodID mid = GetClassMethodId(
      env, cls, className, "<init>", ctorSignature, INSTANCE);
  if (mid == NULL) {
    return JniResult(CheckException(env));
  }

  va_list args;
  va_start(args, ctorSignature);
  JniResult result;
  result.value.l = env->NewObjectV(cls, mid, args);
  va_end(args);
  if (result.value.l == NULL) {
    result.SetCode(CheckException(env));
    HBASE_LOG_ERROR("Unable to create Java object with signature %s(%s)",
                    className, ctorSignature);
  }
  result.SetCode(CheckException(env));

  return result;
}

JniResult
JniHelper::CreateJavaByteArray(
    JNIEnv *env,
    const byte_t *buf,
    const jsize start,
    const jsize len) {
  JniResult result;
  if (buf == NULL || len < 0) {
    result.SetCode(EINVAL);
  } else {
    result.value.l = env->NewByteArray(len);
    if (result.value.l == NULL) {
      HBASE_LOG_ERROR("Unable to create Java byte[] with length %d", len);
      result.SetCode(CheckException(env));
    } else {
      env->SetByteArrayRegion(
          (jbyteArray)result.value.l, start, len, (const jbyte *)buf);
    }
  }
  return result;
}

Status
JniHelper::CreateByteArray(
    JNIEnv *env,
    jobject byteArray,
    byte_t **bufPtr,
    size_t *bufLen) {
  if (byteArray) {
    jbyteArray javaByteArray = static_cast<jbyteArray>(byteArray);
    *bufLen = (size_t) env->GetArrayLength(javaByteArray);
    *bufPtr = new byte_t[*bufLen];
    if (*bufPtr == NULL) {
      *bufLen = 0;
      return Status::ENoMem;
    }
    env->GetByteArrayRegion(javaByteArray, 0, *bufLen, (jbyte*)*bufPtr);
  } else {
    *bufPtr = NULL;
    *bufLen = 0;
  }
  return Status::Success;
}

const Status
JniHelper::SetField(
    JNIEnv *env,
    const char *className,
    const char *fieldName,
    const char *fieldSignature,
    jobject val,
    jobject obj) {
  jclass cls = GetClassReference(env, className);
  if (cls == NULL) {
    return Status(CheckException(env));
  }

  if (obj == NULL) {
    jfieldID fid = env->GetStaticFieldID(cls, fieldName, fieldSignature);
    if (fid == NULL) {
      return Status(CheckException(env));
    }
    env->SetObjectField(obj, fid, val);
  } else {
    jfieldID fid = env->GetFieldID(cls, fieldName, fieldSignature);
    if (fid == NULL) {
      return Status(CheckException(env));
    }
    env->SetObjectField(obj, fid, val);
  }

  return Status::Success;
}

JniResult
JniHelper::CreateJniObjectList(
    JNIEnv *env,
    JniObject **jniObjects,
    size_t numObjects) {
  JniResult list;
  if (numObjects <= 0 || jniObjects == NULL) {
    list.SetCode(EINVAL);
    return list;
  }

  list = JniHelper::NewObject(
      env, JAVA_ARRAYLIST, "(I)V", numObjects);
  if (UNLIKELY(!list.ok())) {
    return list;
  }

  for (size_t i = 0; i < numObjects; ++i) {
    JniResult result = InvokeMethod(
        env, list.GetObject(), JAVA_ARRAYLIST, "add",
        JMETHOD1(JPARAM(JAVA_OBJECT), "Z"), jniObjects[i]->JObject());
    if (UNLIKELY(!result.ok())) {
      return result;
    }
  }
  return list;
}

} /* namespace hbase */
