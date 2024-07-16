/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include <stdint.h>
#include "common/math/simd_util.h"

#if defined(USE_SIMD)

/**
 * ?__m256i?????????????32????
 * 
 * ???????SSE4.1???????256???????????????32????
 * ?????????????AVX??????????????????
 * 
 * @param vec ??__m256i????????????32????
 * @param i ??????????????????????
 * @return ????????32????
 */
int mm256_extract_epi32_var_indx(const __m256i vec, const unsigned int i)
{
  // ???i?????128??????????_mm256_permutevar8x32_epi32???
  __m128i idx = _mm_cvtsi32_si128(i);
  
  // ?????????????256??????????????????????
  __m256i val = _mm256_permutevar8x32_epi32(vec, _mm256_castsi128_si256(idx));
  
  // ?256??????128?????????32?????????
  // ????_mm256_permutevar8x32_epi32????????????????????????
  return _mm_cvtsi128_si32(_mm256_castsi256_si128(val));
}

int mm256_sum_epi32(const int *values, int size)
{ //code 
  int sum = 0;
  for (int i = 0; i < size / 8 * 8; i += 8) {
    __m256i v = _mm256_loadu_si256((__m256i*)(values + i));
    v = _mm256_hadd_epi32(v, v);
    v = _mm256_hadd_epi32(v, v);
    int temp[8];
    _mm256_storeu_si256((__m256i*)temp, v);
    sum += temp[0] + temp[1] + temp[2] + temp[3];
  }
  for (int i = size / 8 * 8; i < size; i++) {
    sum += values[i];
  }
  return sum;
}

float mm256_sum_ps(const float *values, int size)
{
  //code
  float sum = 0.0f;
  for (int i = 0; i < size / 8 * 8; i += 8) {
    __m256 v = _mm256_loadu_ps(values + i);
    v = _mm256_hadd_ps(v, v);
    v = _mm256_hadd_ps(v, v);
    float temp[8];
    _mm256_storeu_ps(temp, v);
    sum += temp[0] + temp[1] + temp[2] + temp[3];
  }
  for (int i = size / 8 * 8; i < size; i++) {
    sum += values[i];
  }
  return sum;
}

template <typename V>
void selective_load(V *memory, int offset, V *vec, __m256i &inv)
{
  int *inv_ptr = reinterpret_cast<int *>(&inv);
  for (int i = 0; i < SIMD_WIDTH; i++) {
    if (inv_ptr[i] == -1) {
      vec[i] = memory[offset++];
    }
  }
}
template void selective_load<uint32_t>(uint32_t *memory, int offset, uint32_t *vec, __m256i &inv);
template void selective_load<int>(int *memory, int offset, int *vec, __m256i &inv);
template void selective_load<float>(float *memory, int offset, float *vec, __m256i &inv);

#endif
