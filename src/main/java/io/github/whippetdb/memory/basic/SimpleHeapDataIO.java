package io.github.whippetdb.memory.basic;

import io.github.whippetdb.util.Util;

public abstract class SimpleHeapDataIO extends HeapDataIO{
   protected int size;
   
   protected abstract void checkWrite(long addr, int len);
   
   protected void checkRead(long addr, int len) {
      Util.checkRead(addr, len, size);
   }
   
   protected void ensureSize(long v) {
      if(v <= size) return;
      ensureCapacity(v);
      size = (int)v;
   }
   
   protected void ensureCapacity(long v) {
      if(v > Integer.MAX_VALUE) throw new IllegalArgumentException("Unsupported capacity: " + v);
      if(data == null) {
         data = new byte[(int)v];
         return;
      }
      if(v <= data.length) return;
      byte[] newdata = new byte[(int) Math.min(Integer.MAX_VALUE, Math.max(v, data.length*2L+16))];
      System.arraycopy(data, 0, newdata, 0, Math.min(data.length,size));
      data = newdata;
   }
   
   @Override
   public String toString() {
      return toString(0, size);
   }
}
