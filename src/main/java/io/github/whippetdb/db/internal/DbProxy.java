package io.github.whippetdb.db.internal;

import io.github.whippetdb.db.api.Db;
import io.github.whippetdb.memory.api.MemIO;

public class DbProxy implements Db{
   protected final Db parent;
   
   public DbProxy(Db parent) {
      this.parent = parent;
   }

   @Override
   public void commit() {
      parent.commit();
   }

   @Override
   public void flush() {
      parent.flush();
   }

   @Override
   public void close() {
      parent.close();
   }

   @Override
   public Integer keySize() {
      return null;
   }

   @Override
   public Integer maxKeySize() {
      return null;
   }

   @Override
   public Integer valueSize() {
      return parent.valueSize();
   }

   @Override
   public Integer maxValueSize() {
      return parent.maxValueSize();
   }

   @Override
   public <T> T seek(MemIO key, long keyOff, int keyLen, SeekHandler<T> handler) {
      return parent.seek(key, keyOff, keyLen, handler);
   }

   @Override
   public <T> T put(MemIO key, long keyOff, int keyLen, PutHandler<T> handler) {
      return parent.put(key, keyOff, keyLen, handler);
   }

   @Override
   public long allocatedSize() {
      return parent.allocatedSize();
   }

   @Override
   public void scan(ScanHandler handler) {
      parent.scan(handler);
   }

   @Override
   public void discard() {
      parent.discard();
   }
}
