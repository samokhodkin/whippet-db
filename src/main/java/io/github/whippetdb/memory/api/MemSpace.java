package io.github.whippetdb.memory.api;

public interface MemSpace extends MemIO{
   public long allocate(int size, boolean clear);
   public long allocatedSize();
}
