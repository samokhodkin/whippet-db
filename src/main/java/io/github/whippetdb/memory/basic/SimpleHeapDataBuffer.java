package io.github.whippetdb.memory.basic;

import io.github.whippetdb.memory.api.MemDataBuffer;
import io.github.whippetdb.util.Util;

public class SimpleHeapDataBuffer extends SimpleHeapDataIO implements MemDataBuffer {
   public SimpleHeapDataBuffer() {}
   
   public SimpleHeapDataBuffer(int size) {
      ensureCapacity(size);
      this.size = size;
   }

   public SimpleHeapDataBuffer(byte... bytes) {
      this(bytes, bytes.length);
   }
   
   public SimpleHeapDataBuffer(byte[] buf, int len) {
      if(buf == null) throw new IllegalArgumentException("buf is null");
      if(buf.length < len) throw new IllegalArgumentException("illegal buf length: " + buf.length + " < " + len);
      data = buf;
      size = len;
   }
   
   
   @Override
   public void setSize(long v) {
      ensureCapacity(v);
      size = (int)v;
   }

   @Override
   public long size() {
      return size;
   }
   
   @Override
   protected void ensureSize(long v) {
      if(v > size) setSize(v);
   }

   @Override
   protected void checkWrite(long addr, int len) {
      Util.checkAppend(addr, size);
      ensureSize(addr+len);
   }
}
