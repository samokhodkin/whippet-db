package io.github.whippetdb.memory.journaling.simple;

import io.github.whippetdb.memory.api.MemDataSpace;

public class SimpleJournalingMemSpace extends SimpleJournalingDataIO implements MemDataSpace {
   protected final MemDataSpace parentSpace;
   
   /** 
    * Currently only parents with matching pageSize must be used. Otherwise:
    * 1. Normal bound check fails
    * 2. If we'd forse a parent to allow cross-boundary IO for last page, 
    *    there would arise a possible inconsistency in allocate();
    * 
    * @param parent
    * @param flush
    * @param log
    * @param lgPageSize
    * @param maxDirtySpace
    * @param maxLogSize
    */
   public SimpleJournalingMemSpace(
      MemDataSpace parent, Runnable flush, int lgPageSize, int maxDirtySpace, long maxLogSize
   ) {
      super(parent, flush, lgPageSize, maxDirtySpace, maxLogSize);
      parentSpace = parent;
   }

   @Override
   public long allocate(int size, boolean clear) {
      // if we have cached a cross-boundary page, there arises an inconcsistency 
      return parentSpace.allocate(size, clear);
      
      // Do we need to explicitly clear the <size>-much bytes using fill()? Will it help?
      //long addr = parentSpace.allocate(size, clear); //false);
      //if(clear) fillLong(addr, size, 0L);
      //return addr;
   }

   @Override
   public long allocatedSize() {
      return parentSpace.allocatedSize();
   }
}
