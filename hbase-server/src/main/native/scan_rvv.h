#ifndef SCAN_RVV_H
#define SCAN_RVV_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * 使用 RISC-V 向量优化的内存比较
 * 返回值：
 *   0：两个内存块相等
 *   非 0：第一个不同字节的差值
 */
int rvv_memcmp(const unsigned char* a, const unsigned char* b, size_t len);

/**
 * 使用 RISC-V 向量优化的内存拷贝
 */
void rvv_memcpy(unsigned char* dst, const unsigned char* src, size_t len);

/**
 * 判断两个内存块前 prefixLen 字节是否相等
 * 返回值：
 *   1：相等
 *   0：不相等
 */
int rvv_prefix_match(const unsigned char* a, const unsigned char* b, size_t prefixLen);

#ifdef __cplusplus
}
#endif

#endif // SCAN_RVV_H

