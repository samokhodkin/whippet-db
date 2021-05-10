package io.github.whippetdb.memory.basic;

import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.util.Arrays;
import java.util.HashSet;

import io.github.whippetdb.memory.api.Bytes;
import io.github.whippetdb.memory.api.MemDataIO;
import io.github.whippetdb.util.FileUtil;
import io.github.whippetdb.util.Util;

import static java.nio.file.StandardOpenOption.*;

import java.io.File;
import java.io.IOException;

public abstract class SimpleFileDataIO extends DirectDataIO implements MemDataIO {
   private File file;
   
   public final MapMode mapMode;
   public final boolean persistent;
   
   protected final FileChannel channel;   
   protected long size;
   
   public SimpleFileDataIO(String path, long initialSize, boolean... persist) throws IOException {
      this(path, MapMode.READ_WRITE, initialSize, persist);
   }
   
   public SimpleFileDataIO(String path, MapMode mapMode, long initialSize, boolean... persist) throws IOException {
      File f = new File(path);
      HashSet<OpenOption> options = new HashSet<>(Arrays.asList(READ, WRITE, CREATE));
      
      if(!f.exists()){
         (endsWithFileSeparator(path)? f: f.getParentFile()).mkdirs();
      }
      
      if(f.isDirectory()) {
         f = Files.createTempFile(f.toPath(), null, null).toFile();
         options.add(DELETE_ON_CLOSE);
      }
      
      if(persist.length > 0) { // forse temp option
         if(persist[0]) {
            options.remove(DELETE_ON_CLOSE);
         }
         else {
            options.add(DELETE_ON_CLOSE);
         }
      }
      
      persistent = !options.contains(DELETE_ON_CLOSE);
      if(!persistent) {
         FileUtil.makeTemporary(f);
      }
      
      this.mapMode = mapMode;
      file = f;
      channel = FileChannel.open(f.toPath(), options);
      ensureCapacity(initialSize);
      size = initialSize;
   }
   
   protected abstract void checkWrite(long addr, int len);
   
   public File file() {
      return file;
   }
   
   public boolean rename(File dest) {
      if(file.renameTo(dest)) {
         file = dest;
         return true;
      }
      return false;
   }
   
   public void flush() {
      if(data != null) {
         ((MappedByteBuffer)data).force();
      }
   }
   
   protected void flush(long size) {
      if(data == null) return;
      size = Math.min(size, data.capacity());
      try {
         MappedByteBuffer bb = channel.map(MapMode.READ_WRITE, 0, size);
         bb.force();
      }
      catch (IOException e) {
         throw new RuntimeException(e);
      }
   }
   
   public void close() {
      try {
         channel.close();
         data = null;
      }
      catch (IOException e) {}
   }
   
   @Override
   public String toString() {
      return toString(0, size);
   }
   
   @Override
   protected void checkRead(long addr, int len) {
      Util.checkRead(addr, len, size);
   }
   
   public void ensureSize(long v) {
      if(v <= size) return;
      ensureCapacity(v);
      size = v;
   }
   
   public void setSize(long v) {
      ensureCapacity(v);
      size = v;
   }
   
   protected void ensureCapacity(long v) {
      if(v > Integer.MAX_VALUE) throw new IllegalArgumentException("Unsupported capacity: " + v + " > " + Integer.MAX_VALUE);
      if(data == null) {
         try {
            data = channel.map(mapMode, 0, Util.ceil2(v, 4L<<10)).order(Bytes.BYTE_ORDER);
            return;
         }
         catch (IOException e) {
            throw new RuntimeException(e);
         }
      }
      
      if(v > data.capacity()) {
         int newCap = (int) Math.min(Integer.MAX_VALUE, Math.max(v, data.capacity()*4L/3));
         try {
            // content is preserved automatically for mapped memory
            data = channel.map(mapMode, 0, newCap).order(Bytes.BYTE_ORDER);
         }
         catch (IOException e) {
            throw new RuntimeException("Mapping " + newCap + " bytes failed", e);
         }
      }
   }

   private static boolean endsWithFileSeparator(String path) {
      return path.endsWith("/") || path.endsWith(File.separator);
   }
}
