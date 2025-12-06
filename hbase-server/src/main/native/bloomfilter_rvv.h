#ifndef _Included_org_apache_hadoop_hbase_util_BloomFilterRvvNative
#define _Included_org_apache_hadoop_hbase_util_BloomFilterRvvNative
#ifdef __cplusplus
extern "C" {
#endif

JNIEXPORT void JNICALL Java_org_apache_hadoop_hbase_util_BloomFilterRvvNative_nativeSetBitOld
  (JNIEnv *, jobject, jbyteArray, jint, jlong);

JNIEXPORT void JNICALL Java_org_apache_hadoop_hbase_util_BloomFilterRvvNative_nativeSetBitsBatch
  (JNIEnv *, jobject, jbyteArray, jint, jlongArray, jint);
JNIEXPORT void JNICALL
Java_org_apache_hadoop_hbase_util_BloomFilterRvvNative_nativeCheckBitsBatch
  (JNIEnv *env, jobject obj, jbyteArray bitmap, jint offset, jlongArray positions, jint length, jbooleanArray results);

#ifdef __cplusplus
}
#endif
#endif

