#include <jni.h>
#include "lz4.h"
#include <stdlib.h>
#include <string.h>

JNIEXPORT jint JNICALL Java_org_apache_hadoop_hbase_io_compress_lz4_Lz4Native_maxCompressedLength
  (JNIEnv *env, jclass clazz, jint srcLen) {
    return LZ4_compressBound(srcLen);
}


JNIEXPORT jint JNICALL Java_org_apache_hadoop_hbase_io_compress_lz4_Lz4Native_compressDirect
  (JNIEnv *env, jclass clazz,
   jobject src, jint srcOff, jint srcLen,
   jobject dst, jint dstOff, jint dstCap) {

    // 取 Direct ByteBuffer 的底层地址
    char* srcPtr = (char*)(*env)->GetDirectBufferAddress(env, src);
    char* dstPtr = (char*)(*env)->GetDirectBufferAddress(env, dst);

    if (srcPtr == NULL || dstPtr == NULL) {
        return -1001; // LZ4_ERROR_NULL_PTR - DirectBuffer 获取失败
    }

    // 偏移量修正
    const char* srcAddr = srcPtr + srcOff;
    char* dstAddr = dstPtr + dstOff;

    // 检查缓冲区边界
    if (srcOff < 0 || srcLen < 0 || dstOff < 0 || dstCap < 0) {
        return -1002; // LZ4_ERROR_INVALID_PARAM - 参数无效
    }
    
    // 检查目标缓冲区是否足够大
    int maxCompressedSize = LZ4_compressBound(srcLen);
    if (dstCap < maxCompressedSize) {
        return -1003; // LZ4_ERROR_BUFFER_TOO_SMALL - 目标缓冲区太小
    }

    // 调用 LZ4 压缩
    int compressedSize = LZ4_compress_default(srcAddr, dstAddr, srcLen, dstCap);

    return compressedSize; // 返回压缩结果大小（失败时 <= 0）
}

JNIEXPORT jint JNICALL Java_org_apache_hadoop_hbase_io_compress_lz4_Lz4Native_decompressDirect
  (JNIEnv *env, jclass clazz,
   jobject src, jint srcOff, jint srcLen,
   jobject dst, jint dstOff, jint dstCap) {

    // 获取 Direct ByteBuffer 的底层地址
    char* srcPtr = (char*)(*env)->GetDirectBufferAddress(env, src);
    char* dstPtr = (char*)(*env)->GetDirectBufferAddress(env, dst);

    if (srcPtr == NULL || dstPtr == NULL) {
        return -1001; // LZ4_ERROR_NULL_PTR - DirectBuffer 获取失败
    }

    // 检查参数有效性
    if (srcOff < 0 || srcLen < 0 || dstOff < 0 || dstCap < 0) {
        return -1002; // LZ4_ERROR_INVALID_PARAM - 参数无效
    }

    // 偏移修正
    const char* srcAddr = srcPtr + srcOff;
    char* dstAddr = dstPtr + dstOff;

    // 调用 LZ4 解压
    int decompressedSize = LZ4_decompress_safe(srcAddr, dstAddr, srcLen, dstCap);

    return decompressedSize; // <0 表示解压失败
}