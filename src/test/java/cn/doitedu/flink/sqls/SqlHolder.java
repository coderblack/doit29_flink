package cn.doitedu.flink.sqls;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

/***
 * @author hunter.d
 * @qq 657270652
 * @wx haitao-duan
 * @date 2022/3/17
 **/
public class SqlHolder {
    public static String getSql(int idx) throws IOException {
        String fileStr = FileUtils.readFileToString(new File("data/sqls/sql.txt"), "utf-8");
        String[] split = fileStr.split("\\~");
        return split[idx-1];
    }
}
