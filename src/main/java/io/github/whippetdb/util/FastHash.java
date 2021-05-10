package io.github.whippetdb.util;

import io.github.whippetdb.memory.api.*;

public class FastHash{
   final static long factor=7540113804746346429L;
   
   public static long hash64(long data){
      long v=data*factor;
      return v^Long.rotateRight(v, 45)^Long.rotateLeft(v,19);
   }
   
   public static long hash64(MemIO str, long start, int len){
      long h=0;
      int off=0;
      for(;len-off>=8;off+=8)  h=hash64(h^Bytes.readLong(str, start+off));
      return off>=len? h: hash64(h^Bytes.readBytes(str, start+off, len-off));
   }
   
   public static long hash64(MemDataIO str, long start, int len){
      long h=0;
      int off=0;
      for(;len-off>=8;off+=8)  h=hash64(h^str.readLong(start+off));
      return off>=len? h: hash64(h^str.readBytes(start+off, len-off));
   }
}
