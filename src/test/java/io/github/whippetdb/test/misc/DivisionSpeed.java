package io.github.whippetdb.test.misc;

import java.util.function.IntConsumer;
import java.util.function.IntFunction;

@SuppressWarnings("unused")
public class DivisionSpeed {
   public static void main(String[] args) {
      IntConsumer testDiv3 = v -> {
         int res = (int)((v*0xAAAAAAABL)>>>33);
         System.out.println(v + " -> " + res);
      };
      IntConsumer testDiv7 = v -> {
         int res = (int)(((v+1)*1227133513L)>>>33);
         System.out.println(v + " -> " + res);
      };
      
      for(int i = 0 ; i < 100; i++) testDiv7.accept(i);
      System.exit(0);
      
      
      long t0, dt;
      
      int s = 0;
      int N = 1<<30;
      
      s = 0;
      t0 = System.currentTimeMillis();
      for(int n = 0; n < N; n++) {
         s = (s + n);
      }
      dt = System.currentTimeMillis() - t0;
      System.out.println(N + " x (): " + dt + ", " + (N*1000.0/dt) + " op/sec");
      
      s = 0;
      t0 = System.currentTimeMillis();
      for(int n = 0; n < N; n++) {
         s = (s + n)*3;
      }
      dt = System.currentTimeMillis() - t0;
      System.out.println(N + " x (*3): " + dt + ", " + (N*1000.0/dt) + " op/sec");
      
      s = 0;
      t0 = System.currentTimeMillis();
      //for(int n = N; n --> 0;) {
      for(int n = 0; n < N; n++) {
         s = (s + n)/3;
      }
      dt = System.currentTimeMillis() - t0;
      System.out.println(N + " x (/3): " + dt + ", " + (N*1000.0/dt) + " op/sec, s=" + s);
      
      s = 0;
      t0 = System.currentTimeMillis();
      //for(int n = N; n --> 0;) {
      for(int n = 0; n < N; n++) {
         s =  (int)(((s+n)*0xAAAAAAABL)>>>33);
      }
      dt = System.currentTimeMillis() - t0;
      System.out.println(N + " x (*0xAAAAAAABL>>33): " + dt + ", " + (N*1000.0/dt) + " op/sec, s=" + s);
      
      s = 0;
      t0 = System.currentTimeMillis();
      for(int n = 0; n < N; n++) {
         s = (s + n)%3;
      }
      dt = System.currentTimeMillis() - t0;
      System.out.println(N + " x (%3): " + dt + ", " + (N*1000.0/dt) + " op/sec");
      
      double u = 0;
      double f = 1.d/3;
      t0 = System.currentTimeMillis();
      for(int n = 0; n < N; n++) {
         u = (u + n)*f;
      }
      dt = System.currentTimeMillis() - t0;
      System.out.println(N + " x (*f): " + dt + ", " + (N*1000.0/dt) + " op/sec");
      
   }
}
