package io.github.whippetdb.memory.api;

public interface MemArray extends MemIO{
   public long size();
   
   default CharSequence readString(long addr) {
      return readString(addr, (int)Math.min(Integer.MAX_VALUE, (size() - addr)>>1));
   }
}
