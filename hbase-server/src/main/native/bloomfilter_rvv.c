// bloomfilter_rvv.c
#include <jni.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#if defined(__riscv) && defined(__riscv_vector)
#include <riscv_vector.h>
#endif

static inline uint8_t bitmask_for_pos(uint8_t bitPos) {
    return (uint8_t)(1u << (bitPos & 7));
}

/* Old single-bi set */
JNIEXPORT void JNICALL
Java_org_apache_hadoop_hbase_util_BloomFilterRvvNative_nativeSetBitOld
  (JNIEnv *env, jobject obj, jbyteArray bitmap, jint offset, jlong pos) {

    if (bitmap == NULL) return;
    jsize bitmap_len = (*env)->GetArrayLength(env, bitmap);
    if (bitmap_len <= 0 || offset < 0 || offset >= bitmap_len) return;

    jbyte *bitmap_ptr = (jbyte*)(*env)->GetPrimitiveArrayCritical(env, bitmap, NULL);
    if (bitmap_ptr == NULL) return;

    uint8_t *buf = (uint8_t *)(bitmap_ptr + offset);
    uint64_t upos = (uint64_t) pos;
    uint64_t bytePos = upos >> 3;
    uint8_t bitPos = (uint8_t)(upos & 0x7);

    if (bytePos < (uint64_t)(bitmap_len - offset)) {
        buf[bytePos] |= bitmask_for_pos(bitPos);
    }

    (*env)->ReleasePrimitiveArrayCritical(env, bitmap, bitmap_ptr, 0);
}

/* Batch set bits */
JNIEXPORT void JNICALL
Java_org_apache_hadoop_hbase_util_BloomFilterRvvNative_nativeSetBitsBatch
  (JNIEnv *env, jobject obj,
   jbyteArray bitmap, jint offset, jlongArray positions, jint length) {

    if (length <= 0 || bitmap == NULL || positions == NULL) return;

    jsize bitmap_len = (*env)->GetArrayLength(env, bitmap);
    if (bitmap_len <= 0 || offset < 0 || offset >= bitmap_len) return;

    int localMaskLen = bitmap_len - offset;
    if (localMaskLen <= 0) return;

    // Copy positions out (no Critical region yet)
    jboolean isCopy = JNI_FALSE;
    jlong* pos_elems = (*env)->GetLongArrayElements(env, positions, &isCopy);
    if (!pos_elems) return;

    uint8_t *localMask = (uint8_t *) calloc((size_t)localMaskLen, 1);
    if (!localMask) { (*env)->ReleaseLongArrayElements(env, positions, pos_elems, JNI_ABORT); return; }

    uint64_t *touched = (uint64_t *) malloc(sizeof(uint64_t) * (size_t)length);
    if (!touched) { free(localMask); (*env)->ReleaseLongArrayElements(env, positions, pos_elems, JNI_ABORT); return; }
    int touchedCount = 0;

#if defined(__riscv) && defined(__riscv_vector)
    size_t max_vl = __riscv_vsetvl_e64m1((size_t)length);
    uint64_t *byte_idx = (uint64_t *) malloc(max_vl * sizeof(uint64_t));
    uint64_t *bit_idx  = (uint64_t *) malloc(max_vl * sizeof(uint64_t));
    if (!byte_idx || !bit_idx) {
        if (byte_idx) free(byte_idx);
        if (bit_idx) free(bit_idx);
        free(touched); free(localMask);
        (*env)->ReleaseLongArrayElements(env, positions, pos_elems, JNI_ABORT);
        return;
    }

    const uint64_t *upos_ptr = (const uint64_t *) pos_elems;
    int i = 0;
    while (i < length) {
        size_t vl = __riscv_vsetvl_e64m1((size_t)(length - i));
        vuint64m1_t vpos = __riscv_vle64_v_u64m1(upos_ptr + i, vl);
        vuint64m1_t vbytePos = __riscv_vsrl_vx_u64m1(vpos, 3, vl);
        vuint64m1_t vbitPos  = __riscv_vand_vx_u64m1(vpos, 7, vl);
        __riscv_vse64_v_u64m1(byte_idx, vbytePos, vl);
        __riscv_vse64_v_u64m1(bit_idx,  vbitPos,  vl);
        for (size_t j = 0; j < vl; j++) {
            uint64_t b = byte_idx[j];
            uint8_t bi = (uint8_t)(bit_idx[j] & 0xFFu);
            if (b < (uint64_t)localMaskLen) {
                uint8_t old = localMask[b];
                localMask[b] = (uint8_t)(old | (uint8_t)(1u << (bi & 7u)));
                if (old == 0) touched[touchedCount++] = b;
            }
        }
        i += (int)vl;
    }
    free(byte_idx);
    free(bit_idx);
#else
    for (int i = 0; i < length; i++) {
        uint64_t upos = (uint64_t)pos_elems[i];
        uint64_t b = upos >> 3;
        uint8_t bi = (uint8_t)(upos & 7u);
        if (b < (uint64_t)localMaskLen) {
            uint8_t old = localMask[b];
            localMask[b] = (uint8_t)(old | (uint8_t)(1u << (bi & 7u)));
            if (old == 0) touched[touchedCount++] = b;
        }
    }
#endif

    // Release positions copy
    (*env)->ReleaseLongArrayElements(env, positions, pos_elems, JNI_ABORT);

    // Short critical region for write-back
    jbyte *bitmap_ptr = (jbyte*)(*env)->GetPrimitiveArrayCritical(env, bitmap, NULL);
    if (bitmap_ptr) {
        uint8_t *buf = (uint8_t *)(bitmap_ptr + offset);
        for (int k = 0; k < touchedCount; k++) {
            uint64_t idx = touched[k];
            buf[idx] |= localMask[idx];
        }
        (*env)->ReleasePrimitiveArrayCritical(env, bitmap, bitmap_ptr, 0);
    }

    free(touched);
    free(localMask);
}

/* Batch check bits */
JNIEXPORT void JNICALL
Java_org_apache_hadoop_hbase_util_BloomFilterRvvNative_nativeCheckBitsBatch
  (JNIEnv *env, jobject obj,
   jbyteArray bitmap, jint offset,
   jlongArray positions, jint length,
   jbooleanArray results) {

    if (length <= 0 || bitmap == NULL || positions == NULL || results == NULL) return;

    jsize bitmap_len = (*env)->GetArrayLength(env, bitmap);
    if (bitmap_len <= 0 || offset < 0 || offset >= bitmap_len) return;

    // Copy arrays out first
    jboolean isCopyP = JNI_FALSE, isCopyR = JNI_FALSE;
    jlong* pos_elems = (*env)->GetLongArrayElements(env, positions, &isCopyP);
    jboolean* res_elems = (*env)->GetBooleanArrayElements(env, results, &isCopyR);
    if (!pos_elems || !res_elems) {
        if (pos_elems) (*env)->ReleaseLongArrayElements(env, positions, pos_elems, JNI_ABORT);
        if (res_elems) (*env)->ReleaseBooleanArrayElements(env, results, res_elems, 0);
        return;
    }

    // Read bitmap briefly in chunks without holding critical for long
    jbyte *bmp = (jbyte*)(*env)->GetPrimitiveArrayCritical(env, bitmap, NULL);
    if (!bmp) {
        (*env)->ReleaseLongArrayElements(env, positions, pos_elems, JNI_ABORT);
        (*env)->ReleaseBooleanArrayElements(env, results, res_elems, 0);
        return;
    }

    uint8_t *buf = (uint8_t *)(bmp + offset);

#if defined(__riscv) && defined(__riscv_vector)
    size_t max_vl = __riscv_vsetvl_e64m1((size_t)length);
    uint64_t *byte_idx = (uint64_t*) malloc(max_vl * sizeof(uint64_t));
    uint64_t *bit_idx  = (uint64_t*) malloc(max_vl * sizeof(uint64_t));
    if (!byte_idx || !bit_idx) {
        if (byte_idx) free(byte_idx);
        if (bit_idx) free(bit_idx);
        (*env)->ReleasePrimitiveArrayCritical(env, bitmap, bmp, 0);
        (*env)->ReleaseLongArrayElements(env, positions, pos_elems, JNI_ABORT);
        (*env)->ReleaseBooleanArrayElements(env, results, res_elems, 0);
        return;
    }

    const uint64_t *upos_ptr = (const uint64_t *) pos_elems;
    int i = 0;
    while (i < length) {
        size_t vl = __riscv_vsetvl_e64m1((size_t)(length - i));
        vuint64m1_t vpos = __riscv_vle64_v_u64m1(upos_ptr + i, vl);
        vuint64m1_t vbytePos = __riscv_vsrl_vx_u64m1(vpos, 3, vl);
        vuint64m1_t vbitPos  = __riscv_vand_vx_u64m1(vpos, 7, vl);
        __riscv_vse64_v_u64m1(byte_idx, vbytePos, vl);
        __riscv_vse64_v_u64m1(bit_idx,  vbitPos,  vl);
        for (size_t j = 0; j < vl; j++) {
            uint64_t b = byte_idx[j];
            uint64_t bi = bit_idx[j];
            if (b < (uint64_t)(bitmap_len - offset)) {
                res_elems[i + j] = (buf[b] & (uint8_t)(1u << (bi & 7))) ? JNI_TRUE : JNI_FALSE;
            } else {
                res_elems[i + j] = JNI_FALSE;
            }
        }
        i += (int)vl;
    }
    free(byte_idx);
    free(bit_idx);
#else
    for (int i = 0; i < length; i++) {
        uint64_t upos = (uint64_t)pos_elems[i];
        uint64_t b = upos >> 3;
        uint8_t bi = (uint8_t)(upos & 7u);
        if (b < (uint64_t)(bitmap_len - offset)) {
            res_elems[i] = (buf[b] & (uint8_t)(1u << (bi & 7))) ? JNI_TRUE : JNI_FALSE;
        } else {
            res_elems[i] = JNI_FALSE;
        }
    }
#endif

    (*env)->ReleasePrimitiveArrayCritical(env, bitmap, bmp, 0);
    (*env)->ReleaseLongArrayElements(env, positions, pos_elems, JNI_ABORT);
    (*env)->ReleaseBooleanArrayElements(env, results, res_elems, 0);
}

static const uint8_t bitvals[8] = { 1u<<0, 1u<<1, 1u<<2, 1u<<3, 1u<<4, 1u<<5, 1u<<6, 1u<<7 };

void set_hashloc_rvv_final(int32_t hash1, int32_t hash2, int32_t byteSize, int hashCount, uint8_t *bloomBuf) {
    if (!bloomBuf || byteSize <= 0 || hashCount <= 0) return;

    const int64_t bloomBitSize = (int64_t)byteSize * 8LL;

    uint8_t *byte_acc = (uint8_t *)calloc(byteSize, sizeof(uint8_t));
    if (!byte_acc) return;

    size_t base = 0;
    while (base < (size_t)hashCount) {
#if defined(__riscv) && defined(__riscv_vector)
        size_t vl = __riscv_vsetvl_e32m1(hashCount - base);
        vuint32m1_t vid_u = __riscv_vid_v_u32m1(vl);
        vint32m1_t vid = __riscv_vreinterpret_v_u32m1_i32m1(vid_u);
        vint32m1_t vIdx = __riscv_vadd_vx_i32m1(vid, base, vl);
        vint32m1_t vMul = __riscv_vmul_vx_i32m1(vIdx, hash2, vl);
        vint32m1_t vComp = __riscv_vadd_vx_i32m1(vMul, hash1, vl);
        int32_t tmp[vl];
        __riscv_vse32_v_i32m1(tmp, vComp, vl);
        for (size_t j = 0; j < vl; ++j) {
            int64_t pos = (int64_t)tmp[j] % bloomBitSize;
            if (pos < 0) pos += bloomBitSize;
            uint32_t bytePos = (uint32_t)(pos >> 3);
            uint32_t bitPos  = (uint32_t)(pos & 0x7);
            byte_acc[bytePos] |= bitvals[bitPos];
        }
        base += vl;
#else
        int32_t pos = (int32_t)base;
        int64_t comp = (int64_t)hash1 + (int64_t)pos * (int64_t)hash2;
        int64_t p = comp % bloomBitSize;
        if (p < 0) p += bloomBitSize;
        uint32_t bytePos = (uint32_t)(p >> 3);
        uint32_t bitPos  = (uint32_t)(p & 0x7);
        byte_acc[bytePos] |= bitvals[bitPos];
        base += 1;
#endif
    }

    for (int i = 0; i < byteSize; ++i) {
        bloomBuf[i] |= byte_acc[i];
    }

    free(byte_acc);
}

JNIEXPORT void JNICALL Java_org_apache_hadoop_hbase_util_BloomFilterRvvNative_addToBloomFinal
  (JNIEnv *env, jobject obj, jbyteArray bloomBuf, jint hash1, jint hash2, jint hashCount) {
    jbyte *buf = (*env)->GetByteArrayElements(env, bloomBuf, NULL);
    jsize len = (*env)->GetArrayLength(env, bloomBuf);

    set_hashloc_rvv_final(hash1, hash2, len, hashCount, (uint8_t *)buf);

    (*env)->ReleaseByteArrayElements(env, bloomBuf, buf, 0);
}

JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_hbase_util_BloomFilterRvvNative_getBitFinal
  (JNIEnv *env, jobject obj, jlong pos, jbyteArray bloomBuf) {

    jsize len = (*env)->GetArrayLength(env, bloomBuf);
    if (pos < 0) return JNI_FALSE;
    int64_t bytePos = pos >> 3;
    if (bytePos >= len) return JNI_FALSE;

    jbyte *buf = (*env)->GetByteArrayElements(env, bloomBuf, NULL);
    int bitPos  = (int)(pos & 0x7);
    int result = ((uint8_t)buf[bytePos] & bitvals[bitPos]) != 0;
    (*env)->ReleaseByteArrayElements(env, bloomBuf, buf, 0);
    return result ? JNI_TRUE : JNI_FALSE;
}
