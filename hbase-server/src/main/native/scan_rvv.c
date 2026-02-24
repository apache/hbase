/**
 * RISC-V RVV 向量化内存比较和拷贝实现
 * 使用 RISC-V 向量扩展指令集优化 HBase Scan 查询性能
 */

#if defined(__riscv) && defined(__riscv_vector)
#include <riscv_vector.h>
#endif
#include <stddef.h>
#include <stdio.h>

/**
 * 使用 RISC-V RVV 指令集实现高效的内存比较
 * 
 * @param a 第一个内存区域指针
 * @param b 第二个内存区域指针  
 * @param len 比较长度
 * @return 比较结果：<0 表示 a<b，=0 表示 a=b，>0 表示 a>b
 *         特殊返回值：-999 表示参数错误
 */
int rvv_memcmp(const unsigned char* a, const unsigned char* b, size_t len) {
    // 边界检查：确保指针有效
    if (!a || !b) return -999;
    if (len == 0) return 0;
    
    size_t i = 0;
#if defined(__riscv) && defined(__riscv_vector)
    // RVV 向量化比较路径
    while (i < len) {
        size_t remaining = len - i;
        // 设置向量长度，处理剩余字节数
        size_t vl = __riscv_vsetvl_e8m1(remaining);
        if (vl == 0) break;
        
        // 向量化加载两个内存区域
        vuint8m1_t va = __riscv_vle8_v_u8m1(a + i, vl);
        vuint8m1_t vb = __riscv_vle8_v_u8m1(b + i, vl);
        
        // 计算异或，找出差异位
        vuint8m1_t vxor = __riscv_vxor_vv_u8m1(va, vb, vl);
        
        // 生成掩码，标记相等位置
        vbool8_t mask_eq = __riscv_vmseq_vx_u8m1_b8(vxor, 0, vl);
        // 反转掩码，标记不相等位置
        vbool8_t mask_diff = __riscv_vmnot_m_b8(mask_eq, vl);
        
        // 快速找到第一个差异位
        int first_diff = __riscv_vfirst_m_b8(mask_diff, vl);
        if (first_diff >= 0) {
            // 找到差异，计算具体位置并返回差值
            size_t pos = i + (size_t)first_diff;
            unsigned char ca = (unsigned char)a[pos];
            unsigned char cb = (unsigned char)b[pos];
            return (int)ca - (int)cb;
        }
        i += vl;  // 移动到下一个向量块
    }
#else
    // 标量回退实现（非 RVV 平台）
    for (; i < len; i++) {
        unsigned char ca = (unsigned char)a[i];
        unsigned char cb = (unsigned char)b[i];
        if (ca != cb) {
            return (int)ca - (int)cb;
        }
    }
#endif
    return 0;  // 所有字节都相等
}

/**
 * 使用 RISC-V RVV 指令集实现高效的内存拷贝
 * 
 * @param dst 目标内存区域指针
 * @param src 源内存区域指针
 * @param len 拷贝长度
 */
void rvv_memcpy(unsigned char* dst, const unsigned char* src, size_t len) {
    // 边界检查：确保指针有效且长度大于0
    if (!dst || !src || len == 0) return;
    
    size_t i = 0;
#if defined(__riscv) && defined(__riscv_vector)
    // RVV 向量化拷贝路径
    while (i < len) {
        size_t remaining = len - i;
        // 设置向量长度，处理剩余字节数
        size_t vl = __riscv_vsetvl_e8m1(remaining);
        if (vl == 0) break;
        
        // 向量化加载源数据
        vuint8m1_t vec = __riscv_vle8_v_u8m1(src + i, vl);
        // 向量化存储到目标位置
        __riscv_vse8_v_u8m1(dst + i, vec, vl);
        i += vl;  // 移动到下一个向量块
    }
#else
    // 标量回退实现（非 RVV 平台）
    for (; i < len; i++) {
        dst[i] = src[i];
    }
#endif
}

/**
 * 使用 RISC-V RVV 指令集实现高效的前缀匹配检查
 * 
 * @param a 第一个内存区域指针
 * @param b 第二个内存区域指针
 * @param prefixLen 前缀长度
 * @return 1 表示前缀匹配，0 表示不匹配
 */
int rvv_prefix_match(const unsigned char* a, const unsigned char* b, size_t prefixLen) {
    // 边界检查：空前缀认为匹配
    if (!a || !b || prefixLen == 0) return 1;
    
    // 复用 rvv_memcmp 实现前缀比较
    return rvv_memcmp(a, b, prefixLen) == 0;
}