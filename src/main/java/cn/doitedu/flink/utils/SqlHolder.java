package cn.doitedu.flink.utils;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

public class SqlHolder {

    public static String getSql(int idx) throws IOException {
        String fileStr = FileUtils.readFileToString(new File("sqls/sqls.sql"), "utf-8");

        return fileStr.split("\\~")[idx-1];
    }

}
