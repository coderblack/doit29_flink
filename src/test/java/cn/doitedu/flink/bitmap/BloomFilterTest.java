//package cn.doitedu.flink.bitmap;
//
//import com.google.common.hash.BloomFilter;
//import com.google.common.hash.Funnels;
//
//public class BloomFilterTest {
//    public static void main(String[] args) {
//
//        BloomFilter<CharSequence> bloomFilter = BloomFilter.create(Funnels.stringFunnel(), 100000000, 0.01);
//
//        bloomFilter.put("abc");
//        bloomFilter.put("bbc");
//        bloomFilter.put("cxc");
//        bloomFilter.put("xxx");
//
//        System.out.println(bloomFilter.mightContain("xxx"));  // true
//        System.out.println(bloomFilter.mightContain("uuy"));  // false
//
//
//    }
//}
