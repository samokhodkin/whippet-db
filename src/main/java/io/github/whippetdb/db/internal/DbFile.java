package io.github.whippetdb.db.internal;

import java.io.File;
import java.io.IOException;

import io.github.whippetdb.db.api.Db;
import io.github.whippetdb.memory.basic.SimpleFileDataSpace;
import io.github.whippetdb.memory.basic.SimpleHeapDataBuffer;
import io.github.whippetdb.memory.journaling.simple.SimpleLog;
import io.github.whippetdb.util.FileUtil;
import io.github.whippetdb.util.Util;

public class DbFile extends SimpleFileDataSpace {
   public static final short VERSION = 0;
   
   private static final byte[] magic = Util.bytes('c','h','e','e','t','a','h','_','d','b');
   
   private static final int MAGIC_OFF = 0;
   private static final int VERSION_OFF = magic.length; // short
   private static final int DB_VERSION_OFF = VERSION_OFF + 2; // short
   private static final int ALIGN_OFF = DB_VERSION_OFF + 2; // byte, log(page size)
   private static final int DB_TYPE_OFF = ALIGN_OFF + 1; // byte, 
   private static final int DB_ROOT_OFF = DB_TYPE_OFF + 1; // long
   private static final int RECORDED_SIZE_OFF = DB_ROOT_OFF + 8;
   
   private static final int HEADER_SIZE = 1024;
   
   public DbFile(String path, Db.Type type, int align) throws IOException {
      super(path, HEADER_SIZE);
      pageSize = 1 << align;
      write(MAGIC_OFF, magic, 0, magic.length);
      writeShort(VERSION_OFF, VERSION);
      writeByte(ALIGN_OFF, align);
      writeByte(DB_TYPE_OFF, type.ordinal());
      writeLong(DB_ROOT_OFF, 0);
      setSize(Util.ceil2(HEADER_SIZE, pageSize));
      writeLong(RECORDED_SIZE_OFF, size);
   }

   public static String defaultLogPath(String path) {
      File f = new File(path);
      return new File(f.getParent(), f.getName() + ".wal").toString();
   }

   public String defaultLogPath() {
      return defaultLogPath(file().getPath());
   }
   
   public DbFile(String path) throws IOException {
      super(path, HEADER_SIZE);
      if(!equals(MAGIC_OFF, new SimpleHeapDataBuffer(magic), 0, magic.length)) throw new IllegalArgumentException("Not a database file: " + path + ", tag=" + toString(0, magic.length));
      if(readShort(VERSION_OFF) != VERSION) throw new IllegalArgumentException("Version mismatch: " + readShort(VERSION_OFF));
      pageSize = 1 << readByte(ALIGN_OFF);
      // set temp. size, don't touch recorded size
      setSize(Util.ceil2(HEADER_SIZE, pageSize));
   }

   public int getVersion() {
      return readShort(VERSION_OFF);
   }
   
   public int getAlign() {
      return readByte(ALIGN_OFF);
   }
   
   public Db.Type getDbType() {
      return Db.Type.values()[readByte(DB_TYPE_OFF)];
   }

   public int getDbVersion() {
      return readShort(DB_VERSION_OFF);
   }
   
   public void setDbVersion(int v) {
      writeShort(DB_VERSION_OFF, v);
   }
   
   public long getDbRoot() {
      return readLong(DB_ROOT_OFF);
   }
   
   public void setDbRoot(long addr) {
      writeLong(DB_ROOT_OFF, addr);
   }
   
   public long getRecordedSize() {
      return readLong(RECORDED_SIZE_OFF);
   }

   public void setRecordedSize(long v) {
      writeLong(RECORDED_SIZE_OFF, v);
   }

   public final long recordedSizeAddr() {
      return RECORDED_SIZE_OFF;
   }
   
   public void mergeLogs(String... paths) throws IOException {
      if(paths.length == 0) {
         mergeLogs(defaultLogPath());
         return;
      }
      for(String path: paths) {
         if(!new File(path).exists()) continue;
         SimpleLog log = new SimpleLog(path, true);
         log.replay(this, this::ensureSize);
         flush();
         log.close();
         FileUtil.remove(path);
      }
   }
   
   // start allocation from recorded size; take into account a potentially changed align.
   public void resume() {
      setSize(Util.ceil2(readLong(RECORDED_SIZE_OFF), 1<<getAlign()));
   }
   
   @Override
   public long allocate(int len, boolean clear) {
      long addr = super.allocate(len, clear);
      writeLong(RECORDED_SIZE_OFF, size);
      return addr;
   }
}
