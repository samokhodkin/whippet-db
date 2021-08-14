package io.github.whippetdb.db.internal;

import java.io.File;
import java.io.IOException;

import io.github.whippetdb.db.api.Db;
import io.github.whippetdb.memory.api.MemDataSpace;
import io.github.whippetdb.memory.api.MemIO;
import io.github.whippetdb.memory.api.MemSpace;
import io.github.whippetdb.memory.basic.SimpleDirectDataSpace;
import io.github.whippetdb.memory.db.MemMap;

public class FixedDbUtil extends DbUtil {
   private static final Runnable ignore = () -> {};
   
   // create in-memory
   public static Db create(int keySize, int valueSize, int maxListSize) {
      MemDataSpace ms = new SimpleDirectDataSpace();
      return wrap(
            ms, new MemMap(ms, keySize, valueSize, maxListSize).new Cursor(ms), keySize, valueSize, 
            false, false, ignore, ignore, ignore, ignore);
   }
   
   public static Db create(String path, int keySize, int valueSize, int maxListSize, int align, boolean autoflush) throws IOException {
      DbFile dbf = new DbFile(path, Db.Type.Fixed, align);
      // get all bits in place
      if(new File(dbf.defaultLogPath()).exists()) dbf.mergeLogs();
      // resume allocation from last recorded size
      dbf.resume();
      
      // create db
      MemMap db = new MemMap(dbf, keySize, valueSize, maxListSize);
      // record db start addr
      dbf.setDbRoot(db.baseAddr);
      // persist changes
      dbf.flush();
      
      return wrap(dbf, db.new Cursor(dbf), keySize, valueSize, false, autoflush, ignore, ignore, dbf::flush, dbf::close);
   }
   
   public static Db createJournaled(
      String path, int keySize, int valueSize, int maxListSize, int align, int maxDirtySize, int maxLogSize,
      boolean autocommit, boolean autoflush
   ) throws IOException {
      DbFile dbf = new DbFile(path, Db.Type.Fixed, align);
      if(new File(dbf.defaultLogPath()).exists()) dbf.mergeLogs();
      dbf.resume();
      
      MemMap db = new MemMap(dbf, keySize, valueSize, maxListSize);
      dbf.setDbRoot(db.baseAddr);
      dbf.flush();
      
      JournalingDbMemSpace mem = new JournalingDbMemSpace(dbf, align, maxDirtySize, maxLogSize);
      return wrap(mem, db.new Cursor(mem), 
            keySize, valueSize, autocommit, autoflush,
            mem::commit, mem::discard, mem::flush, mem::close);
   }
   
   public static Db open(String path, boolean autoflush) throws IOException {
      DbFile dbf = new DbFile(path);
      if(new File(dbf.defaultLogPath()).exists()) dbf.mergeLogs();
      dbf.resume();
      
      // load db
      MemMap db = new MemMap(dbf, dbf.getDbRoot());
      
      return wrap(dbf, db.new Cursor(dbf), 
            db.keySize, db.valueSize, false, autoflush,
            ignore, ignore, dbf::flush, dbf::close);
   }
   
   public static Db openJournaled(String path, int maxDirtySize,
         long maxLogSize, boolean autocommit, boolean autoflush) throws IOException {
      DbFile dbf = new DbFile(path);
      
      // Page size not defined on creation.
      // Can we assign/change it when opening an exising file? Probably yes, but not sure.
      // If yes, the Dbf.resume() should take the changed alignment into account. 
      // TODO: check this.
      if(dbf.getAlign() == 0) throw new IllegalArgumentException("The file doesn't support journaling: " + path + ", page size not defined");
      
      if(new File(dbf.defaultLogPath()).exists()) dbf.mergeLogs();
      dbf.resume();
      
      MemMap db = new MemMap(dbf, dbf.getDbRoot());
      
      JournalingDbMemSpace mem = new JournalingDbMemSpace(dbf, dbf.getAlign(), maxDirtySize, maxLogSize);
      return wrap(mem, db.new Cursor(mem), 
            db.keySize, db.valueSize, autocommit, autoflush,
            mem::commit, mem::discard, mem::flush, mem::close);
   }
   
   static Db wrap(
         MemSpace ms, MemMap.Cursor db, int keySize, int valueSize, boolean autocommit, boolean autoflush, 
         Runnable commit, Runnable discard, Runnable flush, Runnable close) {
      Runnable delete = db::delete;
      return new Db() {
         public <T> T seek(MemIO key, long keyOff, int keyLen, SeekHandler<T> handler) {
            T t = db.seek(key, keyOff)? 
                  handler.run(db.value, delete): 
                  handler.run(null, null);
            if(autocommit) commit.run();
            return t;
         }
         public <T> T put(MemIO key, long keyOff, int keyLen, PutHandler<T> handler) {
            T t = handler.run(db.put(key, keyOff), db.value, delete);
            if(autocommit) commit.run();
            if(autoflush) flush.run();
            return t;
         }
         public void scan(ScanHandler handler) {
            db.scan(() -> {
               return handler.run(db.key, db.value, delete);
            });
            // scan handler might have changed the data
            if(autocommit) commit.run();
            if(autoflush) flush.run();
         }
         public void commit() {
            commit.run();
         }
         public void discard() {
            discard.run();
         }
         public void flush() {
            flush.run();
         }
         public void close() {
            close.run();
         }
         public Integer keySize() {
            return keySize;
         }
         public Integer valueSize() {
            return valueSize;
         }
         public long allocatedSize() {
            return ms.allocatedSize();
         }
      };
   }
}
