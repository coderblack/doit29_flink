package cn.doitedu.flink.bitmap;

import org.roaringbitmap.RoaringBitmap;

public class BitMapTest {

    public static void main(String[] args) {

        RoaringBitmap bm1 = new RoaringBitmap();
        bm1.add(1,3,5);


        RoaringBitmap bm2 = new RoaringBitmap();
        bm2.add(2,3,6);

        // 求一个bitmap（它是 bm1 和  bm2 中共同存在的元素）
        bm1.and(bm2);

        System.out.println(bm1.contains(1));  // false
        System.out.println(bm1.contains(3));  // true


    }

}
