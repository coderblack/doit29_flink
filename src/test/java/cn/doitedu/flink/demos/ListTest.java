package cn.doitedu.flink.demos;

import java.util.ArrayList;

public class ListTest {
    public static void main(String[] args) {

        ArrayList<String> lst = new ArrayList<>();
        lst.add("a");
        lst.add("b");
        lst.add("c");
        lst.add("d");
        lst.add("d");

        for(int i=0;i<2;i++){
            lst.remove(0);
            System.out.println(lst);
        }


    }
}
