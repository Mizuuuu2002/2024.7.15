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

int mm256_extract_epi32_var_indx(const __m256i vec, const unsigned int i)
{
  __m128i idx = _mm_cvtsi32_si128(i);
  __m256i val = _mm256_permutevar8x32_epi32(vec, _mm256_castsi128_si256(idx));
  return _mm_cvtsi128_si32(_mm256_castsi256_si128(val));
}

int mm256_sum_epi32(const int *values, int size)
{
  if(size<16){
    int sum=0;
    for(int i=0;i<size;i++){
      sum+=values[i];
    }
    return sum;
  }
  int epoch=size/16;//????
  int temp[epoch*8+size-epoch*16];//?????
  for(int j=0 ; j<epoch ; j++){
    __m256i temp1 = _mm256_loadu_epi32(values[0+j*16]);//?8?
    __m256i temp2 = _mm256_loadu_epi32(values[0+j*16+8]);//?8?
    __m256i temp3  = _mm256_add_epi32(temp1, temp2);
    _mm256_storeu_epi32(temp[0+j*8], temp3);//??????
  }
  for(int i=epoch*16,j=0;i<size;i++,j++){
    temp[j+epoch*8]=value[i];
  }
  mm256_sum_epi32(temp,epoch*8+size-epoch*16);
}

float mm256_sum_ps(const float *values, int size)
{
  if(size<16){
    float sum=0;
    for(int i=0;i<size;i++){
      sum+=values[i];
    }
    return sum;
  }
  int epoch=size/16;//????
  float temp[epoch*8+size-epoch*16];//?????
  for(int j=0 ; j<epoch ; j++){
    __m256 temp1 = _mm256_loadu_ps(values[0+j*16]);//?8?
    __m256 temp2 = _mm256_loadu_ps(values[0+j*16+8]);//?8?
    __m256 temp3  = _mm256_add_ps(temp1, temp2);
    _mm256_storeu_ps(temp[0+j*8], temp3);//??????
  }
  for(int i=epoch*16,j=0;i<size;i++,j++){
    temp[j+epoch*8]=value[i];
  }
  mm256_sum_epi32(temp,epoch*8+size-epoch*16);
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
