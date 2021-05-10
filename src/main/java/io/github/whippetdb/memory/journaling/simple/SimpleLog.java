package io.github.whippetdb.memory.journaling.simple;

import java.io.IOException;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

import io.github.whippetdb.memory.api.MemDataBuffer;
import io.github.whippetdb.memory.api.MemIO;
import io.github.whippetdb.memory.basic.ProxyDataIO;
import io.github.whippetdb.memory.basic.SimpleFileDataIO;
import io.github.whippetdb.memory.basic.SimpleHeapDataBuffer;
import io.github.whippetdb.util.FastHash;
import io.github.whippetdb.util.Util;

public class SimpleLog extends SimpleFileDataIO {
   final static int FRAME_SEED_OFF = 0;
   final static int FRAME_SIZE_OFF = FRAME_SEED_OFF + 8;
   final static int FRAME_DATA_OFF = FRAME_SIZE_OFF + 4;
   
   final static int RECORD_ADDR_OFF = 0;
   final static int RECORD_SIZE_OFF = RECORD_ADDR_OFF + 8;
   final static int RECORD_DATA_OFF = RECORD_SIZE_OFF + 4;
   
   private long frameAddr = 0;
   private long recordAddr = 0;
   private long flushed = 0;
   
   public SimpleLog(String path, boolean... persist) throws IOException {
      super(path, 8, persist);
   }

   public void start() {
      ensureSize(FRAME_DATA_OFF + 8); // header + next seed
      frameAddr = 0;
      recordAddr = FRAME_DATA_OFF;
      writeLong(FRAME_SEED_OFF, System.currentTimeMillis());
      writeInt(FRAME_SIZE_OFF, FRAME_DATA_OFF); // headr only
      writeLong(FRAME_DATA_OFF, 0); // ensure hash mismatch
      flush();
   }
   
   // log must be started
   public long currentFrame() {
      checkStarted();
      return frameAddr;
   }
   
   // log must be started
   public long alloc(long dstAddr, int size) {
      checkStarted();
      ensureSize(recordAddr + RECORD_DATA_OFF + size);
      writeLong(recordAddr + RECORD_ADDR_OFF, dstAddr);
      writeInt(recordAddr + RECORD_SIZE_OFF, size);
      long addr = recordAddr + RECORD_DATA_OFF;
      recordAddr += RECORD_DATA_OFF + size;
      return addr;
   }
   
   // seal current frame
   public void commit() {
      checkStarted();
      ensureSize(recordAddr + FRAME_DATA_OFF + 8); // space for next header and hash
      writeInt(frameAddr + FRAME_SIZE_OFF, (int)(recordAddr - frameAddr));
      writeLong(recordAddr + FRAME_SEED_OFF, FastHash.hash64(this, frameAddr, (int)(recordAddr - frameAddr)));
      writeInt(recordAddr + FRAME_SIZE_OFF, FRAME_DATA_OFF);
      writeLong(recordAddr + FRAME_DATA_OFF, 0); // ensure hash mismatch for the next frame
      frameAddr = recordAddr;
      recordAddr = frameAddr + FRAME_DATA_OFF;
   }
   
   // discard current frame
   // log must be started
   public void discard() {
      checkStarted();
      ensureSize(recordAddr + FRAME_DATA_OFF + 8); // space for next header and hash
      writeInt(frameAddr + FRAME_SIZE_OFF, FRAME_DATA_OFF); // size = only header
      writeLong(frameAddr + FRAME_DATA_OFF, 0L); // ensure hash mismatch
      recordAddr = frameAddr + FRAME_DATA_OFF;
   }
   
   // log must be started
   public void flush() {
      checkStarted();
      flush((flushed = frameAddr) + FRAME_DATA_OFF + 8); // written data + next frame header
   }
   
   public long dirtySize() {
      return frameAddr - flushed;
   }
   
   public long maxTargetSize() {
      long[] max = new long[1];
      scan(
         null,
         data -> max[0]=Math.max(max[0], data[3] + data[2]),
         null, null
      );
      return max[0];
   }
   
   public void replay(MemIO dst, LongConsumer... ensureSize) {
      scan(
         null,
         data -> {
            long dstAddr = data[3];
            long loggedAddr = data[1];
            int len = (int) data[2];
            if(ensureSize.length > 0) ensureSize[0].accept(dstAddr + len);
            dst.write(dstAddr, this, loggedAddr, len);
         },
         null,
         msg -> System.out.println("  Warning! " + msg)
      );
   }
   
   public void print() {
      System.out.println(file() + ":");
      long[] max = new long[1];
      scan(
         data -> System.out.printf("  Frame at %d, size=%d\n", data[0], data[1]),
         data -> {
            System.out.printf("    Record at %d, data addr: %d, data size: %d, dest addr: %d\n", data[0], data[1], data[2], data[3]);
            max[0]=Math.max(max[0], data[3] + data[2]);
         },
         msg -> System.out.println("  " + msg),
         msg -> System.out.println("  Warning! " + msg)
      );
      System.out.println("  Max target size: " + max[0]);
   }
   
   /**
    * 
    * @param onFrame - frame address, size
    * @param onDataRecord - record address, data address, data size, dest address 
    * @param onIllegalFrameSize - frame address, size
    * @param onHashMismatch - frame address, size, expected hash, actual hash
    * @param onIllegalRecord - frame address, frame size, record address
    */
   public void scan(
      Consumer<long[]> onFrame, Consumer<long[]> onDataRecord, 
      Consumer<String> onMsg, Consumer<String> onWarning
   ){
      long frame = 0;
      for(;;) {
         ensureSize(frame + FRAME_SIZE_OFF + 4);
         int frameSize = readInt(frame + FRAME_SIZE_OFF);
         if(onFrame != null) onFrame.accept(new long[]{frame, frameSize});
         
         if(frameSize < FRAME_DATA_OFF) {
            if(onMsg != null) onMsg.accept("Illegal frame size: " + frameSize);
            return;
         }
         
         ensureSize(frame + frameSize + 8);
         
         long actualHash = FastHash.hash64(this, frame, frameSize);
         long expectedHash = readLong(frame + frameSize);
         
         if(actualHash != expectedHash) {
            if(onMsg != null) onMsg.accept("Hash mismatch: " + actualHash + " != " + expectedHash);
            return;
         }
         
         long record = frame + FRAME_DATA_OFF;
         while(record < frame + frameSize) {
            long dstAddr = readLong(record + RECORD_ADDR_OFF);
            int len = readInt(record + RECORD_SIZE_OFF);
            if(onDataRecord != null) onDataRecord.accept(new long[]{record, record + RECORD_DATA_OFF, len, dstAddr});
            record += RECORD_DATA_OFF + len;
            if(record > frame + frameSize) { // something went wrong
               if(onWarning != null) onWarning.accept("Record outside frame: frame addr: " + frameAddr + ", frame size: " + frameSize + ", record addr: " + record);
               return;
            }
         }
         
         frame += frameSize;
      }
   }

   @Override
   protected void checkWrite(long addr, int len) {
      Util.checkWrite(addr, len, size);
   }
   
   protected void checkStarted() {
      if(recordAddr < frameAddr + FRAME_DATA_OFF) {
         throw new IllegalStateException("not started");
      }
   }
   
   public String toString_d() {
      return "{frameAddr="+frameAddr+", recordAddr="+recordAddr+", size="+size+", data="+this+"}";
   }
   
   public static void main(String[] args) throws IOException {
      SimpleLog log = new SimpleLog("tmp/SimpleLog.test");
      System.out.println("=== Before start ===");
      log.print();
      log.start();
      System.out.println("=== After start ===");
      log.print();
      log.writeLong(log.alloc(10, 8), 11);
      log.writeLong(log.alloc(20, 8), 22);
      log.writeLong(log.alloc(30, 8), 33);
      log.commit();
      log.writeLong(log.alloc(40, 8), 11);
      log.writeLong(log.alloc(50, 8), 22);
      log.writeLong(log.alloc(60, 8), 33);
      log.commit();
      System.out.println("=== After commit ===");
      log.print();
      
      MemDataBuffer dst = new SimpleHeapDataBuffer();
      System.out.println("Dst before replay: " + dst);
      log.replay(new ProxyDataIO() {
         protected long offset() {
            throw new Error();
         }
         protected void checkRead(long addr, int len) {}
         protected void checkWrite(long addr, int len) {}
         @Override
         public void write(long addr, MemIO src, long srcAddr, int len) {
            Util.ensureSize(dst, addr + len);
            dst.write(addr, src, srcAddr, len);
         }
      });
      System.out.println("Dst after replay: " + dst);
      
   }
}
