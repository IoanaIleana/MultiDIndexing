#include <sys/types.h>
#include <stdint.h>
#include "constants.h"




uint64_t one64 = (uint64_t)1;
uint64_t zero64 = (uint64_t)0;
uint64_t max64 = ~zero64;
uint64_t mod64mask=(uint64_t)63;

uint64_t masks64[(int)64] = {
  one64<<(int)0, one64<<(int)1, one64<<(int)2, one64<<(int)3, one64<<(int)4, one64<<(int)5, one64<<(int)6, one64<<(int)7,
  one64<<(int)8, one64<<(int)9, one64<<(int)10, one64<<(int)11, one64<<(int)12, one64<<(int)13, one64<<(int)14, one64<<(int)15,
  one64<<(int)16, one64<<(int)17, one64<<(int)18, one64<<(int)19, one64<<(int)20, one64<<(int)21, one64<<(int)22, one64<<(int)23,
  one64<<(int)24, one64<<(int)25, one64<<(int)26, one64<<(int)27, one64<<(int)28, one64<<(int)29, one64<<(int)30, one64<<(int)31,
  one64<<(int)32, one64<<(int)33, one64<<(int)34, one64<<(int)35, one64<<(int)36, one64<<(int)37, one64<<(int)38, one64<<(int)39,
  one64<<(int)40, one64<<(int)41, one64<<(int)42, one64<<(int)43, one64<<(int)44, one64<<(int)45, one64<<(int)46, one64<<(int)47,
  one64<<(int)48, one64<<(int)49, one64<<(int)50, one64<<(int)51, one64<<(int)52, one64<<(int)53, one64<<(int)54, one64<<(int)55,
  one64<<(int)56, one64<<(int)57, one64<<(int)58, one64<<(int)59, one64<<(int)60, one64<<(int)61, one64<<(int)62, one64<<(int)63
  };
