package io.github.whippetdb.util;

import io.github.whippetdb.memory.api.*;

public class FastHash{
   final static long factor=7540113804746346429L;
   final static int STEP = 5;
   
//   public static long hash64old(long data){
//      long v=data*factor;
//      return v^Long.rotateRight(v, 45)^Long.rotateLeft(v,19);
//   }
//   
//   public static long hash64(MemIO str, long start, int len){
//      long h=0;
//      int off=0;
//      for(;len-off>=8;off+=STEP)  h=hash64(h^Bytes.readLong(str, start+off));
//      return off>=len? h: hash64(h^Bytes.readBytes(str, start+off, len-off));
//   }
//   
//   public static long hash64(MemDataIO str, long start, int len){
//      long h=0;
//      int off=0;
//      for(;len-off>=8;off+=STEP)  h=hash64(h^str.readLong(start+off));
//      return off>=len? h: hash64(h^str.readBytes(start+off, len-off));
//   }
   
   /*
    * Improver version based on FNB hash
    */
   
   public static long hash64(long v) {
      return (-5808582161921358473L + v) * 1099511628211L;
   }
   
   public static long hash64(MemDataIO data, long off, int len) {
      long end = len;
      long h = (0xcbf29ce484222325L ^ end) * 1099511628211L;
      while(end >= 8) {
         h += data.readLong(off + end-8);
         h *= 1099511628211L;
         end -= 8;
      }
      if(end > 0) {
         h += data.readBytes(off, (int) end);
         h *= 1099511628211L;
      }
      return h;
   }
   
   public static long hash64(MemIO data, long off, int len) {
      long end = len;
      long h = (0xcbf29ce484222325L ^ end) * 1099511628211L;
      while(end >= 8) {
         h += Bytes.readLong(data, end-8);
         h *= 1099511628211L;
         end -= 8;
      }
      if(end > 0) {
         h += Bytes.readBytes(data, 0, (int) end);
         h *= 1099511628211L;
      }
      return h;
   }
   
   public static long hash64(MemDataArray mem){
      return hash64(mem, 0, (int) mem.size());
   }
}
