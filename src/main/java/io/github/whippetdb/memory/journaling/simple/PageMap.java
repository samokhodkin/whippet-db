package io.github.whippetdb.memory.journaling.simple;

import io.github.whippetdb.util.AbstractLongMap;

class PageMap extends AbstractLongMap<PageMap.Entry>{
   static final long NULL = -1;
   
   static class Entry extends AbstractLongMap.Entry {
      // Address of a page in the log (journal)
      long loggedAddr = NULL;
      
      // The page was commited previously to the log, and not yet requested for write.
      // When this page is requested for write, this flag is set to false.
      boolean commited;
      
      // When a write is requested, the new page is allocated on the log and existing address is saved here
      long prevLoggedAddr;
      
      @Override
      public String toString() {
         return key + "->" + loggedAddr + "(" + commited + ", " + prevLoggedAddr + ")";
      }
   }

   protected Entry alloc() {
      return new Entry();
   }
}
