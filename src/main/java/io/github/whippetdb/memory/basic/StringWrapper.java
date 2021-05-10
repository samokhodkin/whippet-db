package io.github.whippetdb.memory.basic;

import io.github.whippetdb.memory.api.MemDataArray;

public class StringWrapper implements MemDataArray {
   protected final int off, size;
   protected final CharSequence data;
   
   public StringWrapper(CharSequence data) {
      this(data, 0, data.length());
   }
   
   public StringWrapper(CharSequence data, int off, int len) {
      this.data = data;
      this.off = off;
      size = data.length() << 1;
   }
   
   @Override
   public int readByte(long addr) {
      int a = (int)addr;
      return (data.charAt(a>>1)>>((a&1)<<3))&0xff;
   }

   @Override
   public void writeByte(long addr, int b) {
      throw new UnsupportedOperationException();
   }
   
   @Override
   public long size() {
      return size;
   }
   
   @Override
   public String toString() {
      return toString(0, size);
   }
}
