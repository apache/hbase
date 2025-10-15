#include <jni.h>  // 包含 rvv_memcmp / rvv_prefix_match 的声明
#include <stdint.h>
#include <stddef.h>
#include "scan_rvv.h"

#if defined(__riscv) && defined(__riscv_vector)
#include <riscv_vector.h>
#endif

// ------------------ JNI 接口 ------------------

/**
 * JNI 接口：使用 RVV 优化的内存比较
 * 比较两个 byte[] 数组的指定区域
 * 
 * @param env JNI 环境指针
 * @param clazz 调用类
 * @param a 第一个字节数组
 * @param offsetA 第一个数组的偏移量
 * @param lengthA 第一个数组的长度
 * @param b 第二个字节数组
 * @param offsetB 第二个数组的偏移量
 * @param lengthB 第二个数组的长度
 * @return 比较结果：<0 表示 a<b，=0 表示 a=b，>0 表示 a>b
 *         特殊返回值：-999 表示参数错误，-997 表示偏移量错误，-998 表示内存分配失败
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_hbase_util_ScanRVV_memcmp
  (JNIEnv *env, jclass clazz,
   jbyteArray a, jint offsetA, jint lengthA,
   jbyteArray b, jint offsetB, jint lengthB) {
    // 参数验证
    if (!a || !b || lengthA <= 0 || lengthB <= 0) {
        return -999;
    }
    size_t len = lengthA < lengthB ? lengthA : lengthB;

    if (offsetA < 0 || offsetB < 0 || offsetA + len > lengthA || offsetB + len > lengthB) {
        return -997;
    }
    
    // 获取数组指针
    jbyte* a_ptr = (*env)->GetPrimitiveArrayCritical(env, a, 0);
    jbyte* b_ptr = (*env)->GetPrimitiveArrayCritical(env, b, 0);
    if (!a_ptr || !b_ptr) {
        return -998;
    }
    
    // 调用 RVV 优化的内存比较
    int ret = rvv_memcmp((const unsigned char*)(a_ptr + offsetA),
                         (const unsigned char*)(b_ptr + offsetB),
                         len);
    
    // 释放数组指针
    (*env)->ReleasePrimitiveArrayCritical(env, a, a_ptr, JNI_ABORT);
    (*env)->ReleasePrimitiveArrayCritical(env, b, b_ptr, JNI_ABORT);

    if (ret != 0) return ret;
    return lengthA - lengthB;
}


/**
 * JNI 接口：使用 RVV 优化的前缀匹配检查
 * 检查两个 byte[] 数组是否具有相同的前缀
 * 
 * @param env JNI 环境指针
 * @param clazz 调用类
 * @param a 第一个字节数组
 * @param offsetA 第一个数组的偏移量
 * @param b 第二个字节数组
 * @param offsetB 第二个数组的偏移量
 * @param prefixLen 前缀长度
 * @return JNI_TRUE 表示前缀匹配，JNI_FALSE 表示不匹配
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_hbase_util_ScanRVV_prefixMatch
  (JNIEnv *env, jclass clazz,
   jbyteArray a, jint offsetA,
   jbyteArray b, jint offsetB,
   jint prefixLen) 
{
    // 参数验证
    if (!a || !b || prefixLen <= 0) return JNI_FALSE;

    jsize lenA = (*env)->GetArrayLength(env, a);
    jsize lenB = (*env)->GetArrayLength(env, b);
    if (offsetA < 0 || offsetB < 0 || offsetA + prefixLen > lenA || offsetB + prefixLen > lenB)
        return JNI_FALSE;

    // 获取数组指针
    jbyte* a_ptr = (*env)->GetPrimitiveArrayCritical(env, a, 0);
    jbyte* b_ptr = (*env)->GetPrimitiveArrayCritical(env, b, 0);

    // 调用 RVV 优化的前缀匹配
    int ret = rvv_prefix_match((const unsigned char*)(a_ptr + offsetA),
                               (const unsigned char*)(b_ptr + offsetB),
                               (size_t)prefixLen);

    // 释放数组指针
    (*env)->ReleasePrimitiveArrayCritical(env, a, a_ptr, JNI_ABORT);
    (*env)->ReleasePrimitiveArrayCritical(env, b, b_ptr, JNI_ABORT);

    return ret ? JNI_TRUE : JNI_FALSE;
}

/**
 * JNI 接口：ByteBuffer vs ByteBuffer 的公共前缀查找
 * 使用 RVV 指令集优化查找两个 ByteBuffer 的公共前缀长度
 * 
 * @param env JNI 环境指针
 * @param clazz 调用类
 * @param a 第一个 ByteBuffer
 * @param aOffset 第一个 ByteBuffer 的偏移量
 * @param aLen 第一个 ByteBuffer 的长度
 * @param b 第二个 ByteBuffer
 * @param bOffset 第二个 ByteBuffer 的偏移量
 * @param bLen 第二个 ByteBuffer 的长度
 * @return 公共前缀的长度
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_hbase_util_RVVByteBufferUtils_findCommonPrefixRvv__Ljava_nio_ByteBuffer_2IILjava_nio_ByteBuffer_2II
  (JNIEnv *env, jclass clazz, jobject a, jint aOffset, jint aLen,
   jobject b, jint bOffset, jint bLen) {

    // 获取 DirectBuffer 地址
    const unsigned char* pa = (const unsigned char*)(*env)->GetDirectBufferAddress(env, a);
    const unsigned char* pb = (const unsigned char*)(*env)->GetDirectBufferAddress(env, b);

    if (!pa || !pb) return 0;

    size_t maxLen = aLen < bLen ? aLen : bLen;
    size_t i = 0;

#if defined(__riscv) && defined(__riscv_vector)
    // RVV 向量化查找公共前缀
    while (i < maxLen) {
        size_t remaining = maxLen - i;
        size_t vl = __riscv_vsetvl_e8m1(remaining);
        if (vl == 0) break;  // 添加 vl 检查

        // 向量化加载
        vuint8m1_t va = __riscv_vle8_v_u8m1(pa + aOffset + i, vl);
        vuint8m1_t vb = __riscv_vle8_v_u8m1(pb + bOffset + i, vl);

        // 计算异或，找出差异位
        vuint8m1_t vxor = __riscv_vxor_vv_u8m1(va, vb, vl);

        // 生成掩码，标记相等位置
        vbool8_t mask = __riscv_vmseq_vx_u8m1_b8(vxor, 0, vl);
        // 反转掩码，标记不相等位置
        vbool8_t mask_diff = __riscv_vmnot_m_b8(mask, vl);

        // 找第一个不同字节的位置
        int first_diff = __riscv_vfirst_m_b8(mask_diff, vl);

        if (first_diff >= 0) {
            i += first_diff;
            return (jint)i;
        }

        i += vl;
    }
    return (jint)i;
#else
    // 标量回退实现
    for (; i < maxLen; i++) {
        if (pa[aOffset + i] != pb[bOffset + i]) break;
    }
    return (jint)i;
#endif
}

/**
 * JNI 接口：ByteBuffer vs byte[] 的公共前缀查找
 * 使用 RVV 指令集优化查找 ByteBuffer 和 byte[] 的公共前缀长度
 * 
 * @param env JNI 环境指针
 * @param clazz 调用类
 * @param a 第一个 ByteBuffer
 * @param aOffset 第一个 ByteBuffer 的偏移量
 * @param aLen 第一个 ByteBuffer 的长度
 * @param b 第二个字节数组
 * @param bOffset 第二个数组的偏移量
 * @param bLen 第二个数组的长度
 * @return 公共前缀的长度
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_hbase_util_RVVByteBufferUtils_findCommonPrefixRvv__Ljava_nio_ByteBuffer_2II_3BII
  (JNIEnv *env, jclass clazz, jobject a, jint aOffset, jint aLen,
   jbyteArray b, jint bOffset, jint bLen) {

    // 获取 DirectBuffer 地址
    const unsigned char* pa = (const unsigned char*)(*env)->GetDirectBufferAddress(env, a);
    if (!pa) return 0;

    // 获取 byte[] 指针
    jbyte* pb = (*env)->GetPrimitiveArrayCritical(env, b, 0);
    if (!pb) return 0;

    size_t maxLen = aLen < bLen ? aLen : bLen;
    size_t i = 0;

#if defined(__riscv) && defined(__riscv_vector)
    // RVV 向量化查找公共前缀
    while (i < maxLen) {
        size_t remaining = maxLen - i;
        size_t vl = __riscv_vsetvl_e8m1(remaining);
        if (vl == 0) break;  // 添加 vl 检查

        // 向量化加载
        vuint8m1_t va = __riscv_vle8_v_u8m1(pa + aOffset + i, vl);
        vuint8m1_t vb = __riscv_vle8_v_u8m1((const unsigned char*)(pb + bOffset + i), vl);

        // 计算异或，找出差异位
        vuint8m1_t vxor = __riscv_vxor_vv_u8m1(va, vb, vl);

        // 生成掩码，标记相等位置
        vbool8_t mask = __riscv_vmseq_vx_u8m1_b8(vxor, 0, vl);
        // 反转掩码，标记不相等位置
        vbool8_t mask_diff = __riscv_vmnot_m_b8(mask, vl);

        // 找第一个不同字节
        int first_diff = __riscv_vfirst_m_b8(mask_diff, vl);

        if (first_diff >= 0) {
            i += first_diff;
            (*env)->ReleasePrimitiveArrayCritical(env, b, pb, JNI_ABORT);
            return (jint)i;
        }

        i += vl;
    }

    (*env)->ReleasePrimitiveArrayCritical(env, b, pb, JNI_ABORT);
    return (jint)i;
#else
    // 标量回退实现
    for (; i < maxLen; i++) {
        if (pa[aOffset + i] != pb[bOffset + i]) break;
    }
    (*env)->ReleasePrimitiveArrayCritical(env, b, pb, JNI_ABORT);
    return (jint)i;
#endif
}

/**
 * JNI 接口：byte[] vs byte[] 的公共前缀查找
 * 使用 RVV 指令集优化查找两个 byte[] 的公共前缀长度
 * 
 * @param env JNI 环境指针
 * @param clazz 调用类
 * @param a 第一个字节数组
 * @param offsetA 第一个数组的偏移量
 * @param lengthA 第一个数组的长度
 * @param b 第二个字节数组
 * @param offsetB 第二个数组的偏移量
 * @param lengthB 第二个数组的长度
 * @return 公共前缀的长度
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_hbase_util_ScanRVV_rvvCommonPrefix
  (JNIEnv *env, jclass clazz,
   jbyteArray a, jint offsetA, jint lengthA,
   jbyteArray b, jint offsetB, jint lengthB) {

    // 获取数组指针
    jbyte* arrA = (*env)->GetPrimitiveArrayCritical(env, a, 0);
    jbyte* arrB = (*env)->GetPrimitiveArrayCritical(env, b, 0);

    size_t maxLen = lengthA < lengthB ? lengthA : lengthB;
    size_t i = 0;

#if defined(__riscv) && defined(__riscv_vector)
    // RVV 向量化查找公共前缀
    const unsigned char* pa = (const unsigned char*)(arrA + offsetA);
    const unsigned char* pb = (const unsigned char*)(arrB + offsetB);

    while (i < maxLen) {
        size_t remaining = maxLen - i;
        size_t vl = __riscv_vsetvl_e8m1(remaining);
        if (vl == 0) break;  // 添加 vl 检查

        // 向量化加载
        vuint8m1_t va = __riscv_vle8_v_u8m1(pa + i, vl);
        vuint8m1_t vb = __riscv_vle8_v_u8m1(pb + i, vl);
        vuint8m1_t vxor = __riscv_vxor_vv_u8m1(va, vb, vl);

        // 生成掩码，标记相等位置
        vbool8_t mask = __riscv_vmseq_vx_u8m1_b8(vxor, 0, vl);
        // 反转掩码，标记不相等位置
        vbool8_t mask_diff = __riscv_vmnot_m_b8(mask, vl);

        // 找第一个不同字节
        int first_diff = __riscv_vfirst_m_b8(mask_diff, vl);

        if (first_diff >= 0) {
            i += first_diff;
            break;
        }

        i += vl;
    }
#else
    // 标量回退实现
    for (; i < maxLen; i++) {
        if (arrA[offsetA + i] != arrB[offsetB + i]) break;
    }
#endif
    // 确保数组被释放并返回值
    (*env)->ReleasePrimitiveArrayCritical(env, a, arrA, JNI_ABORT);
    (*env)->ReleasePrimitiveArrayCritical(env, b, arrB, JNI_ABORT);
    return (jint)i;
}

/**
 * JNI 接口：使用 RVV 优化的内存拷贝
 * 在 byte[] 数组之间进行高效的内存拷贝
 * 
 * @param env JNI 环境指针
 * @param clazz 调用类
 * @param dst 目标字节数组
 * @param dstOffset 目标数组的偏移量
 * @param src 源字节数组
 * @param srcOffset 源数组的偏移量
 * @param length 拷贝长度
 */
JNIEXPORT void JNICALL Java_org_apache_hadoop_hbase_util_ScanRVV_rvvMemcpy
  (JNIEnv *env, jclass clazz,
   jbyteArray dst, jint dstOffset,
   jbyteArray src, jint srcOffset, jint length) {

    // 参数验证
    if (!dst || !src || length <= 0) {
        return;
    }

    // 获取数组指针
    jbyte* dst_ptr = (*env)->GetPrimitiveArrayCritical(env, dst, 0);
    jbyte* src_ptr = (*env)->GetPrimitiveArrayCritical(env, src, 0);
    if (!dst_ptr || !src_ptr) {
        return;
    }

#if defined(__riscv) && defined(__riscv_vector)
    // RVV 向量化拷贝
    size_t i = 0;
    while (i < (size_t)length) {
        size_t remaining = length - i;
        size_t vl = __riscv_vsetvl_e8m1(remaining);
        if (vl == 0) break;  // 添加 vl 检查
        
        // 向量化加载和存储
        vuint8m1_t vec = __riscv_vle8_v_u8m1((const unsigned char*)(src_ptr + srcOffset + i), vl);
        __riscv_vse8_v_u8m1((unsigned char*)(dst_ptr + dstOffset + i), vec, vl);
        i += vl;
    }
#else
    // 标量回退实现
    for (size_t i = 0; i < (size_t)length; i++) {
        dst_ptr[dstOffset + i] = src_ptr[srcOffset + i];
    }
#endif

    // 释放数组指针
    (*env)->ReleasePrimitiveArrayCritical(env, dst, dst_ptr, 0);
    (*env)->ReleasePrimitiveArrayCritical(env, src, src_ptr, JNI_ABORT);
}

/**
 * JNI 接口：使用 RVV 优化的字节数组比较
 * 比较两个 byte[] 数组的指定区域
 * 
 * @param env JNI 环境指针
 * @param clazz 调用类
 * @param left 左字节数组
 * @param loff 左数组的偏移量
 * @param llen 左数组的长度
 * @param right 右字节数组
 * @param roff 右数组的偏移量
 * @param rlen 右数组的长度
 * @return 比较结果：<0 表示 left<right，=0 表示 left=right，>0 表示 left>right
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_hbase_util_Bytes_compareToRvv
  (JNIEnv *env, jclass clazz,
   jbyteArray left, jint loff, jint llen,
   jbyteArray right, jint roff, jint rlen) {

    // 获取数组指针
    jbyte* l = (*env)->GetPrimitiveArrayCritical(env, left, 0);
    jbyte* r = (*env)->GetPrimitiveArrayCritical(env, right, 0);

    size_t maxLen = llen < rlen ? llen : rlen;
    size_t i = 0;

#if defined(__riscv) && defined(__riscv_vector)
    // RVV 向量化比较
    while (i < maxLen) {
        size_t remaining = maxLen - i;
        size_t vl = __riscv_vsetvl_e8m1(remaining);
        if (vl == 0) break;  // 添加 vl 检查

        // 使用 unsigned 类型加载
        vuint8m1_t vlv = __riscv_vle8_v_u8m1((const unsigned char*)(l + loff + i), vl);
        vuint8m1_t vrv = __riscv_vle8_v_u8m1((const unsigned char*)(r + roff + i), vl);

        // 计算异或，找出差异位
        vuint8m1_t vxor = __riscv_vxor_vv_u8m1(vlv, vrv, vl);

        // 生成掩码，标记相等位置
        vbool8_t mask = __riscv_vmseq_vx_u8m1_b8(vxor, 0, vl);
        // 反转掩码，标记不相等位置
        vbool8_t mask_diff = __riscv_vmnot_m_b8(mask, vl);
        int first_diff = __riscv_vfirst_m_b8(mask_diff, vl);

        if (first_diff >= 0) {
            int ret = ((unsigned char)l[loff + i + first_diff]) -
                        ((unsigned char)r[roff + i + first_diff]);
            (*env)->ReleasePrimitiveArrayCritical(env, left, l, 0);
            (*env)->ReleasePrimitiveArrayCritical(env, right, r, 0);
            return ret;
        }

        i += vl;
    }
#else
    // 标量回退实现
    for (; i < maxLen; i++) {
        if (l[loff + i] != r[roff + i]) {
            int ret = (int)(l[loff + i]) - (int)(r[roff + i]);
            (*env)->ReleasePrimitiveArrayCritical(env, left, l, 0);
            (*env)->ReleasePrimitiveArrayCritical(env, right, r, 0);
            return ret;
        }
    }
#endif

    // 释放数组指针
    (*env)->ReleasePrimitiveArrayCritical(env, left, l, 0);
    (*env)->ReleasePrimitiveArrayCritical(env, right, r, 0);
    return 0;
}



/**
 * JNI 接口：检查 RVV 是否可用
 * 返回当前平台是否支持 RISC-V 向量扩展
 * 
 * @param env JNI 环境指针
 * @param clazz 调用类
 * @return JNI_TRUE 表示支持 RVV，JNI_FALSE 表示不支持
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_hbase_util_ScanRVV_isEnabled
  (JNIEnv* env, jclass clazz) {
#if defined(__riscv) && defined(__riscv_vector)
    return JNI_TRUE;
#else
    return JNI_FALSE;
#endif
}

/**
 * JNI 接口：全量 Cell 比较
 * 使用 RVV 优化比较两个完整的 Cell 键
 * 
 * @param env JNI 环境指针
 * @param clazz 调用类
 * @param aKey 第一个 Cell 键
 * @param aLen 第一个键的长度
 * @param bKey 第二个 Cell 键
 * @param bLen 第二个键的长度
 * @return 比较结果：<0 表示 aKey<bKey，=0 表示 aKey=bKey，>0 表示 aKey>bKey
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_hbase_util_ScanRVV_compareCells
  (JNIEnv* env, jclass clazz, jbyteArray aKey, jint aLen, jbyteArray bKey, jint bLen) {

    // 获取数组指针
    jbyte* a_ptr = (*env)->GetPrimitiveArrayCritical(env, aKey, 0);
    jbyte* b_ptr = (*env)->GetPrimitiveArrayCritical(env, bKey, 0);

    // 比较最小长度部分
    size_t minLen = aLen < bLen ? aLen : bLen;
    int cmp = rvv_memcmp((const unsigned char*)a_ptr, (const unsigned char*)b_ptr, minLen);
    if (cmp == 0) cmp = (int)aLen - (int)bLen;

    // 释放数组指针
    (*env)->ReleasePrimitiveArrayCritical(env, aKey, a_ptr, JNI_ABORT);
    (*env)->ReleasePrimitiveArrayCritical(env, bKey, b_ptr, JNI_ABORT);

    return cmp;
}

/**
 * JNI 接口：比较键以确定是否跳转到下一行
 * 使用 RVV 优化比较索引键和当前键
 * 
 * @param env JNI 环境指针
 * @param clazz 调用类
 * @param indexedKey 索引键
 * @param idxLen 索引键长度
 * @param curKey 当前键
 * @param curLen 当前键长度
 * @return 比较结果：<0 表示 indexedKey<curKey，=0 表示 indexedKey=curKey，>0 表示 indexedKey>curKey
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_hbase_util_ScanRVV_compareKeyForNextRow
  (JNIEnv* env, jclass clazz, jbyteArray indexedKey, jint idxLen, jbyteArray curKey, jint curLen) {

    // 获取数组指针
    jbyte* idx_ptr = (*env)->GetPrimitiveArrayCritical(env, indexedKey, 0);
    jbyte* cur_ptr = (*env)->GetPrimitiveArrayCritical(env, curKey, 0);

    // 比较最小长度部分
    size_t minLen = idxLen < curLen ? idxLen : curLen;
    int cmp = rvv_memcmp((const unsigned char*)idx_ptr, (const unsigned char*)cur_ptr, minLen);
    if (cmp == 0) cmp = (int)idxLen - (int)curLen;

    // 释放数组指针
    (*env)->ReleasePrimitiveArrayCritical(env, indexedKey, idx_ptr, JNI_ABORT);
    (*env)->ReleasePrimitiveArrayCritical(env, curKey, cur_ptr, JNI_ABORT);

    return cmp;
}

/**
 * JNI 接口：比较键以确定是否跳转到下一列
 * 使用 RVV 优化比较索引键和当前键
 * 
 * @param env JNI 环境指针
 * @param clazz 调用类
 * @param indexedKey 索引键
 * @param idxLen 索引键长度
 * @param curKey 当前键
 * @param curLen 当前键长度
 * @return 比较结果：<0 表示 indexedKey<curKey，=0 表示 indexedKey=curKey，>0 表示 indexedKey>curKey
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_hbase_util_ScanRVV_compareKeyForNextColumn
  (JNIEnv* env, jclass clazz, jbyteArray indexedKey, jint idxLen, jbyteArray curKey, jint curLen) {

    // 获取数组指针
    jbyte* idx_ptr = (*env)->GetPrimitiveArrayCritical(env, indexedKey, 0);
    jbyte* cur_ptr = (*env)->GetPrimitiveArrayCritical(env, curKey, 0);

    // 比较最小长度部分
    size_t minLen = idxLen < curLen ? idxLen : curLen;
    int cmp = rvv_memcmp((const unsigned char*)idx_ptr, (const unsigned char*)cur_ptr, minLen);
    if (cmp == 0) cmp = (int)idxLen - (int)curLen;

    // 释放数组指针
    (*env)->ReleasePrimitiveArrayCritical(env, indexedKey, idx_ptr, JNI_ABORT);
    (*env)->ReleasePrimitiveArrayCritical(env, curKey, cur_ptr, JNI_ABORT);

    return cmp;
}
