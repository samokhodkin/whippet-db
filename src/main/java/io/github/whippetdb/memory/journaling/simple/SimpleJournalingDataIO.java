package io.github.whippetdb.memory.journaling.simple;

import static io.github.whippetdb.memory.journaling.simple.PageMap.Entry;
import static io.github.whippetdb.memory.journaling.simple.PageMap.NULL;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import io.github.whippetdb.memory.api.MemDataIO;
import io.github.whippetdb.memory.api.MemSpace;
import io.github.whippetdb.memory.basic.DynamicDataIO;
import io.github.whippetdb.util.AbstractLongMap;
import io.github.whippetdb.util.FileUtil;

public abstract class SimpleJournalingDataIO extends DynamicDataIO implements MemDataIO {
   static final int LOG_NATIVE_PAGE_SIZE = 12;
   
   protected SimpleLog log; // a journal; should be assigned by a subclass
   protected final MemDataIO parent;
   protected final int lgPageSize, pageSize; // 'lg' means 'logarithm base 2'
   protected final long pageMask;
   protected final int maxDirtySpace;
   protected final long maxLogSize;
   
   protected final PageMap pageMap = new PageMap();
   private final ArrayList<Entry> changedEntries = new ArrayList<>();
   private final Runnable flush;
   
   /**
    * The log must be started!
    */
   public SimpleJournalingDataIO(MemDataIO parent, Runnable flush, int lgPageSize, int maxDirtySpace, long maxLogSize) {
      super();
      this.parent = parent;
      this.flush = flush;
      this.lgPageSize = lgPageSize;
      this.pageSize = 1<<lgPageSize;
      this.pageMask = pageSize - 1;
      this.maxDirtySpace = maxDirtySpace;
      this.maxLogSize = maxLogSize;
   }
   
   public void commit() {
      int numChangedEntries = changedEntries.size();
      if(numChangedEntries == 0) return;
      
      log.commit();
      for(int i = numChangedEntries; i --> 0;) {
         Entry e = changedEntries.get(i);
         e.prevLoggedAddr = NULL;
         e.commited = true;
      }
      changedEntries.clear();
      
      if(log.dirtySize() >= maxDirtySpace) {
         flushPages();
         //flushPagesDefault();
      }
   }
   
   public void discard() {
      log.discard();
      for(int i = changedEntries.size(); i --> 0;) {
         Entry e = changedEntries.get(i);
         if((e.loggedAddr = e.prevLoggedAddr) == NULL)  pageMap.remove(e);
         else {
            e.prevLoggedAddr = NULL;
            e.commited = true;
         }
      }
      changedEntries.clear();
   }
   
   public void flush() {
      log.flush(); 
   }
   
   public void close() {
      changedEntries.forEach(e -> pageMap.remove(e));
      pageMap.scan(e -> parent.write(e.key << lgPageSize, log, e.loggedAddr, pageSize));
      flush.run();
      pageMap.clear();
      
      log.start();
      log.close();
      try {
         FileUtil.remove(log.file().getPath());
         log = null;
      }
      catch (IOException e) {}
   }
   
   SimpleLog log() {
      return log;
   }
   
   @Override
   protected void arrangeRead(long addr) {
      long pageId = addr >>> lgPageSize;
      Entry pe = pageMap.get(pageId);
      if(pe == null) {
         buffer = parent;
         bufferAddr = addr;
         bufferLen = (int) (pageSize - (addr & pageMask));
         return;
      }
      buffer = log;
      bufferAddr = pe.loggedAddr + (addr & pageMask);
      bufferLen = (int) (pageSize - (addr & pageMask));
   }
   
   @Override
   protected void arrangeWrite(long addr) {
      long pageId = addr >>> lgPageSize;
      Entry e = pageMap.put(pageId);
      
      long loggedPageAddr;
      if(e.loggedAddr == NULL) { // new entry
         long pageAddr = addr & ~pageMask;
         loggedPageAddr = log.alloc(pageAddr, pageSize);
         log.write(e.loggedAddr = loggedPageAddr, parent, pageAddr, pageSize);
         e.commited = false;
         changedEntries.add(e);
      }
      else if(e.commited) {
         e.prevLoggedAddr = e.loggedAddr;
         long pageAddr = addr & ~pageMask;
         loggedPageAddr = log.alloc(pageAddr, pageSize);
         log.write(e.loggedAddr = loggedPageAddr, log, e.prevLoggedAddr, pageSize);
         e.commited = false;
         changedEntries.add(e);
      }
      else loggedPageAddr = e.loggedAddr;
      
      buffer = log;
      bufferAddr = loggedPageAddr + (addr & pageMask);
      bufferLen = (int) (pageSize - (addr & pageMask));
   }
   
   protected void flushPages() {
      log.flush();
      if(log.currentFrame() >= maxLogSize) {
         final int fillThreshold = 1 << (LOG_NATIVE_PAGE_SIZE - lgPageSize - 1); // half the number of log pages in a native page
         final long logedAddrThreshold = maxLogSize*3/10;
         
         NativePageMap nativePageMap = new NativePageMap();
         // dense enough pages go to main memory
         pageMap.scan(p->{
            nativePageMap.put((p.key << lgPageSize)>>>LOG_NATIVE_PAGE_SIZE).pages.add(p);
         });

         ArrayList<NativePageMap.Entry> natPages = new ArrayList<>();
         nativePageMap.scan(e -> natPages.add(e));
         natPages.sort((e1,e2) -> Long.compare(e1.key, e2.key));
         natPages.forEach(e -> {
            e.pages.sort((p1,p2) -> Long.compare(p1.key, p2.key));
            if(e.pages.size() >= fillThreshold){
               for(int i = 0; i < e.pages.size(); i++) {
                  PageMap.Entry p = e.pages.get(i);
                  parent.write(p.key << lgPageSize, log, p.loggedAddr, pageSize);
                  pageMap.remove(p);
               }
            }
            else {
               for(int i = 0; i < e.pages.size(); i++) {
                  PageMap.Entry p = e.pages.get(i);
                  if(p.loggedAddr <= logedAddrThreshold) {
                     parent.write(p.key << lgPageSize, log, p.loggedAddr, pageSize);
                     pageMap.remove(p);
                  }
               }
            }
         });
         
         flush.run();
         
         // rotate log
         try{
            SimpleLog newLog = new SimpleLog(log.file().getParent(), true); // random persistent file in the same dir
            newLog.start();
            if(pageMap.size() > 0) {
               pageMap.scan(p -> {
                  long a = newLog.alloc(p.key << lgPageSize, pageSize);
                  newLog.write(a, log, p.loggedAddr, pageSize);
                  p.loggedAddr = a;
               });
               newLog.commit();
               newLog.flush();
            }
            
            File currentFile = log.file();
            File newFile = newLog.file();
            File tmpFile = new File(currentFile.getPath() + ".0");
            if(!(
               log.rename(tmpFile) &&
               newLog.rename(currentFile) &&
               log.rename(newFile)
            )) throw new RuntimeException(
               "Couldn't rotate log files: failed to rename " + currentFile + " to " + tmpFile + ", " + 
               newLog.file() + " to " + currentFile + " and " + tmpFile + " to " + newFile
            );
            
            log.close();
            log = newLog;
            
            FileUtil.remove(newFile.getPath());
         }
         catch (IOException e) {
            throw new RuntimeException(e);
         }
      }
   }
   
   @Override
   protected void checkWrite(long addr, int len) {}
   
   @Override
   protected void checkRead(long addr, int len) {}
   
   @Override
   public String toString() {
      return toString(0, ((MemSpace)parent).allocatedSize());
   }
   
   static class NativePageMap extends AbstractLongMap<NativePageMap.Entry> {
      static class Entry extends AbstractLongMap.Entry {
         final ArrayList<PageMap.Entry> pages = new ArrayList<>();
      }
      protected Entry alloc() {
         return new Entry();
      }
   }
}
