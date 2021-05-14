package io.github.whippetdb.util;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Map;
import java.util.function.LongFunction;

import io.github.whippetdb.db.api.DbBuilder;
import io.github.whippetdb.db.api.types.BooleanIO;
import io.github.whippetdb.db.api.types.LongIO;
import io.github.whippetdb.memory.api.Bytes;
import io.github.whippetdb.memory.api.MemDataArray;
import io.github.whippetdb.memory.api.MemDataIO;
import io.github.whippetdb.memory.basic.StringWrapper;

public class FastHashProblem {
   @SuppressWarnings("unused")
   public static void main(String[] args) throws Exception {
      MessageDigest md = MessageDigest.getInstance("MD5");
      int N = 10_000_000;
      
//      System.out.println("hash64(i): " + Arrays.toString(stat(N, i -> FastHash.hash64(i))));
//      System.out.println("md(i): " + Arrays.toString(stat(N, i -> md(md,i))));
//      System.out.println("fnv(i): " + Arrays.toString(stat(N, i -> fnv(i))));
//      System.out.println("sdbm(i): " + Arrays.toString(stat(N, i -> sdbm(i))));
//      System.out.println("djb2(i): " + Arrays.toString(stat(N, i -> djb2(i))));
//      
//      System.out.println("hash64(''+i): " + Arrays.toString(stat(N, i -> hash64(""+i))));
//      System.out.println("md(''+i): " + Arrays.toString(stat(N, i -> md(md,""+i))));
//      System.out.println("fnv(''+i): " + Arrays.toString(stat(N, i -> fnv(""+i))));
//      System.out.println("sdbm(''+i): " + Arrays.toString(stat(N, i -> sdbm(""+i))));
//      System.out.println("djb2(''+i): " + Arrays.toString(stat(N, i -> djb2(""+i))));
//      
//      System.out.println("hash64(''+hash64(i)): " + Arrays.toString(stat(N, i -> hash64(""+FastHash.hash64(i)))));
//      System.out.println("md(''+hash64(i)): " + Arrays.toString(stat(N, i -> md(md,""+FastHash.hash64(i)))));
//      System.out.println("fnv(''+hash64(i)): " + Arrays.toString(stat(N, i -> fnv(""+FastHash.hash64(i)))));
//      System.out.println("sdbm(''+hash64(i)): " + Arrays.toString(stat(N, i -> sdbm(""+FastHash.hash64(i)))));
//      System.out.println("djb2(''+hash64(i)): " + Arrays.toString(stat(N, i -> djb2(""+FastHash.hash64(i)))));
    
//      System.out.println("hash64(i): " + Arrays.toString(stat(N, 64, i -> FastHash.hash64(i))));
//      System.out.println("fnv(i): " + Arrays.toString(stat(N, 64, i -> fnv(i))));
      
      //System.out.println("hash64(hash64(i)): " + Arrays.toString(stat(N, 64, i -> FastHash.hash64(FastHash.hash64(i)))));
      System.out.println("hash64(i*hash64(i)): " + Arrays.toString(stat(N, 64, i -> FastHash.hash64(i*FastHash.hash64(i)))));
      
//      System.out.println("hash64(''+i): " + Arrays.toString(stat(N, 64, i -> hash64(""+i))));
//      System.out.println("fnv(''+i): " + Arrays.toString(stat(N, 64, i -> fnv(""+i))));
   }
   
   static float[] stat(int N, LongFunction<Long> hash) {
      return stat(N,32,hash);
   }
   static float[] stat(int N, int maxBits, LongFunction<Long> hash) {
      System.out.println("stat(" + N + ")");
      long[] hashes = new long[N];
      for(int i = N; i --> 0;) {
         hashes[i] = hash.apply(i);
      }
      
      float[] stat = new float[maxBits];
      Map<Long,Boolean> map = new DbBuilder<>(new LongIO(), new BooleanIO()).create().asMap();
      
      for(int bits=1; bits <= maxBits; bits++) {
         System.out.println("  processing " + bits + " bits");
         long mask = (1L<<bits)-1;
         map.clear();
         for(int i = N; i --> 0;) {
            map.put(hashes[i] & mask, true);
         }
         long size = map.size();
         float fill = size * 1f / N;
         stat[bits-1] = fill;
         if(fill == 1f) {
            for(int i = bits; i < stat.length; i++) stat[i] = 1f;
            break;
         }
      }
      
      return stat;
   }
   
   static long hash64(String s) {
      return FastHash.hash64(new StringWrapper(s));
   }
   
   static long fnv(String s) {
      return fnv(new StringWrapper(s), 0, s.length()<<1);
   }
   static long fnv(MemDataIO data, long off, int len) {
      long end = len;
      long h = (0xcbf29ce484222325L ^ end) * 1099511628211L;
      int step = 5;
      while(end >= 8) {
         h += data.readLong(end-8);
         h *= 1099511628211L;
         end -= step;
      }
      if(end > 0) {
         h += data.readBytes(0, (int) end);
         h *= 1099511628211L;
      }
      return h;
   }
   
   static long fnv(long v) {
      return ((v*0xcbf29ce484222325L)+(Long.rotateRight(v,40)))*1099511628211L;
   }
   
   static long md(MessageDigest md, long v) {
      byte[] data = new byte[8];
      Bytes.writeLong(data, 0, v);
      byte[] hash = md.digest(data);
      return Bytes.readLong(hash, 0);
   }
   
   static long md(MessageDigest md, String s) {
      ByteBuffer bb = ByteBuffer.allocate(s.length()<<1);
      bb.asCharBuffer().append(s);
      byte[] hash = md.digest(bb.array());
      return Bytes.readLong(hash, 0);
   }
   
   static long sdbm(long v) {   
       long hash = 0;
       for(int i = 0; i < 8; i++) {
          hash = ((v>>(i<<3))&0xff) + (hash << 6) + (hash << 16) - hash;
       }
       return hash;
   }
   
   static long sdbm(String s) {   
      MemDataArray data = new StringWrapper(s);
      long hash = 0;
      for(int i = 0; i < data.size(); i++) {
         hash = data.readByte(i) + (hash << 6) + (hash << 16) - hash;
      }
      return hash;
   }
   
   static long djb2(long v) {
      long hash = 5381;
      for(int i = 0; i < 8; i++) {
         hash = ((hash << 5) + hash) + ((v>>(i<<3))&0xff);
      }
      return hash;
   }   
   
   static long djb2(String s) {
      MemDataArray data = new StringWrapper(s);
      long hash = 5381;
      for(int i = 0; i < data.size(); i++) {
         hash = ((hash << 5) + hash) + data.readByte(i);
      }
      return hash;
   }   
}
