package io.github.whippetdb.db.internal;

import java.io.File;
import java.io.IOException;

import io.github.whippetdb.db.api.Db;
import io.github.whippetdb.memory.api.MemDataSpace;
import io.github.whippetdb.memory.api.MemIO;
import io.github.whippetdb.memory.api.MemSpace;
import io.github.whippetdb.memory.basic.SimpleDirectDataSpace;
import io.github.whippetdb.memory.db.VarMap;
import io.github.whippetdb.util.Util;

public class VarDbUtil extends DbUtil {
   private static final Runnable ignore = () -> {};
   
   // create in-memory
   public static Db create(int avgKeySize, int avgValueSize, int maxListSize) {
      MemDataSpace ms = new SimpleDirectDataSpace();
      return wrap(ms, new VarMap(ms, avgKeySize, avgValueSize, maxListSize).new Cursor(ms), false, false, ignore, ignore, ignore, ignore);
   }
   
   public static Db create(String path, int avgKeySize, int avgValueSize, int maxListSize, int align, boolean autoflush) throws IOException {
      DbFile dbf = new DbFile(path, Db.Type.Fixed, align);
      // get all bits in place
      if(new File(dbf.defaultLogPath()).exists()) dbf.mergeLogs();
      // resume allocation from last recorded size
      dbf.resume();
      
      // create db
      VarMap db = new VarMap(dbf, avgKeySize, avgValueSize, maxListSize); 
      // record db start addr
      dbf.setDbRoot(db.baseAddr);
      // persist changes
      dbf.flush();
      
      return wrap(dbf, db.new Cursor(dbf), false, autoflush, ignore, ignore, dbf::flush, dbf::close);
   }
   
   public static Db createJournaled(
      String path, int keySize, int valueSize, int maxListSize, int align, int maxDirtySize, int maxLogSize, 
      boolean autocommit, boolean autoflush
   ) throws IOException {
      DbFile dbf = new DbFile(path, Db.Type.Fixed, align);
      new File(dbf.defaultLogPath()).delete();
      dbf.resume();
      
      VarMap db = new VarMap(dbf, Util.ceil2(keySize, align), Util.ceil2(valueSize, align), maxListSize);
      dbf.setDbRoot(db.baseAddr);
      dbf.flush();
      
      JournalingDbMemSpace mem = new JournalingDbMemSpace(dbf, align, maxDirtySize, maxLogSize);
      return wrap(mem, db.new Cursor(mem), autocommit, autoflush, mem::commit, mem::discard, mem::flush, mem::close);
   }
   
   public static Db open(String path, boolean autoflush) throws IOException {
      DbFile dbf = new DbFile(path);
      if(new File(dbf.defaultLogPath()).exists()) dbf.mergeLogs();
      dbf.resume();
      
      // load db
      VarMap db = new VarMap(dbf, dbf.getDbRoot());
      
      return wrap(dbf, db.new Cursor(dbf), false, autoflush, ignore, ignore, dbf::flush, dbf::close);
   }
   
   public static Db openJournaled(
         String path, int maxDirtySize, long maxLogSize, 
         boolean autocommit, boolean autoflush) throws IOException {
      DbFile dbf = new DbFile(path);
      if(new File(dbf.defaultLogPath()).exists()) dbf.mergeLogs();
      dbf.resume();
      
      VarMap db = new VarMap(dbf, dbf.getDbRoot());
      
      JournalingDbMemSpace mem = new JournalingDbMemSpace(dbf, dbf.getAlign(), maxDirtySize, maxLogSize);
      return wrap(mem, db.new Cursor(mem), autocommit, autoflush, mem::commit, mem::discard, mem::flush, mem::close);
   }
   
   static Db wrap(
         MemSpace ms, VarMap.Cursor db, boolean autocommit, boolean autoflush,
         Runnable commit, Runnable discard, Runnable flush, Runnable close) {
      Runnable delete = db::delete;
      return new Db() {
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
            return null;
         }
         public Integer valueSize() {
            return null;
         }
         public long allocatedSize() {
            return ms.allocatedSize();
         }
         public <T> T seek(MemIO key, long keyOff, int keyLen, SeekHandler<T> handler) {
            T t = db.seek(key, keyOff, keyLen)? handler.run(db.value, delete): handler.run(null, null);
            if(autocommit) commit.run();
            return t;
         }
         public <T> T put(MemIO key, long keyOff, int keyLen, PutHandler<T> handler) {
            T t = handler.run(db.put(key, keyOff, keyLen), db.value, delete);
            if(autocommit) commit.run();
            if(autoflush) flush.run();
            return t;
         }
         public void scan(ScanHandler handler) {
            db.scan(k -> {
               return handler.run(k, db.value, delete);
            });
            if(autocommit) discard.run();
         }
      };
   }
}
