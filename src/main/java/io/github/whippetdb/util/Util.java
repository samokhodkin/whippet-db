package io.github.whippetdb.util;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Objects;

import io.github.whippetdb.memory.api.MemBuffer;

public class Util {
   public static long ceil2(long x){
      x -= 1;
      x |= x >>> 1;
      x |= x >>> 2;
      x |= x >>> 4;
      x |= x >>> 8;
      x |= x >>> 16;
      x |= x >>> 32;
      return x+1;
   }
   
   public static int ceil2(int x){
      x -= 1;
      x |= x >>> 1;
      x |= x >>> 2;
      x |= x >>> 4;
      x |= x >>> 8;
      x |= x >>> 16;
      return x+1;
   }
   
   public static int ceil2(int v, int pow2){
      int mask=pow2-1;
      return v+((~((v&mask)-1))&mask);
   }
   
   public static long ceil2(long v, long pow2){
      long mask=pow2-1;
      return v+((~((v&mask)-1))&mask);
   }
   
   public static int log2(long x){
      x -= 1;
      if(x < 0) return -1;
      int n = 0;
      if((x&0xffffffff00000000L) != 0) {n += 32; x >>>= 32;};
      if((x&0xffff0000L) != 0) {n += 16; x >>>= 16;};
      if((x&0xff00L) != 0) {n += 8; x >>>= 8;};
      if((x&0xf0L) != 0) {n += 4; x >>>= 4;};
      if((x&12) != 0) {n += 2; x >>>= 2;};
      if((x&2) != 0) {n += 1; x >>>= 1;};
      return x == 1? n + 1: n;
   }
   
   public static boolean isPow2(long i){
      return (i&~(i^(i-1)))==0;
   }
   
   public static int sign2(int i) {
      return i < 0? -1: i == 0? 0: 1;
   }
   
   public static int sign2(long i) {
      return i < 0? -1: i == 0? 0: 1;
   }
   
   public static void fillByte(ByteBuffer bb, int off, int len, int b) {
      long v = 0x0101010101010101L * b;
      while(len >= 8) {
         bb.putLong(off, v);
         off += 8;
         len -= 8;
      }
      while(len --> 0) {
         bb.put(off++, (byte)b);
      }
   }
   
   public static void copy(ByteBuffer src, int srcOff, ByteBuffer dst, int dstOff, int len) {
      (dst = dst.duplicate()).clear().position(dstOff);
      (src = src.duplicate()).position(srcOff).limit(srcOff + len);
      dst.put(src);
   }
   
   public static String toString(int[] data, int off, int len){
      StringBuilder sb=new StringBuilder("[");
      if(len>0){
         sb.append(data[off]);
         for(int i=1;i<len;i++) sb.append(',').append(data[off+i]);
      } 
      return sb.append("]").toString();
   }
   
   public static String toString(long[] data, int off, int len){
      StringBuilder sb=new StringBuilder("[");
      if(len>0){
         sb.append(data[off]);
         for(int i=1;i<len;i++) sb.append(',').append(data[off+i]);
      } 
      return sb.append("]").toString();
   }
   
   public static String toString(ByteBuffer data, int off, int len){
      StringBuilder sb=new StringBuilder("[");
      if(len>0){
         sb.append(data.get(off));
         for(int i=1;i<len;i++) sb.append(',').append(data.get(off+i));
      } 
      return sb.append("]").toString();
   }
   
   public static void checkRead(long addr, int len, long size) {
      if(addr < 0) throw new IndexOutOfBoundsException("Reading at negative addr: " + addr);
      if(addr + len > size) throw new IndexOutOfBoundsException("Reading past boundary: " + addr + " + " + len + " > " + size);
   }
   
   public static void checkWrite(long addr, int len, long size) {
      if(addr < 0) throw new IndexOutOfBoundsException("Writing to negative addr: " + addr);
      if(addr + len > size) throw new IndexOutOfBoundsException("Writing past boundary: " + addr + " + " + len + " > " + size);
   }
   
   public static void checkAppend(long addr, long size) {
      if(addr < 0) throw new IllegalArgumentException("Writing to negative address: " + addr);
      if(addr > size) throw new IllegalArgumentException("Non-continous write: " + addr + " > " + size);
   }
   
   public static void ensureSize(MemBuffer mem, long size) {
      if(mem.size() < size) mem.setSize(size);
   }
   
   public static byte[] bytes(int... b) {
      byte[] buf = new byte[b.length];
      for(int i = 0; i < b.length; i++) buf[i] = (byte)b[i];
      return buf;
   }

   public static void notNull(Object obj, String name) {
      if(obj == null) throw new IllegalArgumentException("Parameter " + name + " is null");
   }

   public static void notNull(long v, String name) {
      if(v == 0) throw new IllegalArgumentException("Parameter " + name + " is zero");
   }
   
   public static void assertEquals(Object a, Object b, String... msg) {
      if(!Objects.equals(a, b)) throw new AssertionError((msg.length > 0? msg[0] + ": ": "") + a + " != " + b);
   }
   
   public static void assertNotEquals(Object a, Object b, String... msg) {
      if(Objects.equals(a, b)) throw new AssertionError((msg.length > 0? msg[0] + ": ": "") + a + " == " + b);
   }
   
   public static Object getField(Object src, String... path){
      for(int i = 0; i < path.length; i++) {
         src = getField0(src, path[i]);
      }
      return src;
   }
   
   public static void setField(Object src, String name, Object value){
      try{
         Field f;
         if(src instanceof Class) f=getFieldRef((Class<?>)src, name);
         else f = getFieldRef(src.getClass(), name);
         
         f.setAccessible(true);
         f.set(src, value);
      }
      catch(Exception e){
         throw new RuntimeException(e);
      }
   }
   
   private static Field getFieldRef(Class<?> cls, String name) {
      while(cls != null) {
         try{
            return cls.getDeclaredField(name);
         }
         catch (Exception e) {
            cls = cls.getSuperclass();
         }
      }
      return null;
   }
   
   public static Object call(Object obj, String methodName, Object... args){
      Class<?> c;
      if(obj instanceof Class) { 
         c = (Class<?>)obj;
         obj = null;
      }
      else {
         c = obj.getClass();
      }
      try{
         while(c!=null){
            for(Method m:c.getDeclaredMethods()){
               if(m.getName().equals(methodName) && m.getParameterTypes().length==args.length){
                  try{
                     m.setAccessible(true);
                     return m.invoke(obj, args);
                  }
                  catch(IllegalArgumentException e){}
               }
            }
            c=c.getSuperclass();
         }
         throw new NoSuchMethodException();
      }
      catch(Exception e){
         throw new RuntimeException("method " + methodName + "() not found in " + obj, e);
      }
   }
   
   
   static Object getField0(Object src, String name){
      try{
         Field f;
         if(src instanceof Class) f=getFieldRef((Class<?>)src, name);
         else f = getFieldRef(src.getClass(), name);
         if(f == null) throw new IllegalArgumentException("field " + name + " not found in " + src);
         
         f.setAccessible(true);
         return f.get(src);
      }
      catch(Exception e){
         throw new RuntimeException(e);
      }
   }
   
   public static void main(String[] args) {
      assertEquals(call(Util.class, "ceil2", 17, 16), 32L);
      
      assertEquals(ceil2(0, 1), 0);
      assertEquals(ceil2(1, 1), 1);
      assertEquals(ceil2(8, 1), 8);
      assertEquals(ceil2(9, 1), 9);
      
      assertEquals(ceil2(0, 8), 0);
      assertEquals(ceil2(1, 8), 8);
      assertEquals(ceil2(8, 8), 8);
      assertEquals(ceil2(9, 8), 16);
      
      assertEquals(log2(-1), -1);
      assertEquals(log2(0), -1);
      assertEquals(log2(1), 0);
      assertEquals(log2(2), 1);
      assertEquals(log2(3), 2);
      assertEquals(log2(4), 2);
      assertEquals(log2(5), 3);
      assertEquals(log2(8), 3);
      assertEquals(log2(9), 4);
      assertEquals(log2(16), 4);
      assertEquals(log2(17), 5);
      
      assertEquals(ceil2(-1), 0);
      assertEquals(ceil2(0), 0);
      assertEquals(ceil2(1), 1);
      assertEquals(ceil2(2), 2);
      assertEquals(ceil2(3), 4);
      assertEquals(ceil2(4), 4);
      assertEquals(ceil2(5), 8);
      assertEquals(ceil2(8), 8);
      assertEquals(ceil2(9), 16);
      assertEquals(ceil2(16), 16);
      assertEquals(ceil2(17), 32);
   }
}
