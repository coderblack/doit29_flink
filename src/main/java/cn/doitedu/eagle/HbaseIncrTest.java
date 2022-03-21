package cn.doitedu.eagle;

import org.apache.commons.lang3.StringUtils;


import java.io.IOException;
import java.util.ArrayList;

/***
 * @author hunter.d
 * @qq 657270652
 * @wx haitao-duan
 * @date 2022/3/21
 **/
public class HbaseIncrTest {
    public static void main(String[] args) throws IOException {
        ArrayList<String> lst = new ArrayList<>();
        for(int i=1;i<=1000;i++){
            lst.add(StringUtils.leftPad(i+"",3,"0"));
        }
        new Thread(new IncrRunnable(lst,"t1 ")).start();
        new Thread(new IncrRunnable(lst,"t2 ")).start();

    }
}
