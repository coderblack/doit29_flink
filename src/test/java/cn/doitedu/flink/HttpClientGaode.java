package cn.doitedu.flink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;

public class HttpClientGaode {

    public static void main(String[] args) throws IOException {


        // 构造一个http的客户端对象
        CloseableHttpClient httpClient = HttpClients.createDefault();


        // 构造一个请求
        HttpGet httpGet = new HttpGet("https://restapi.amap.com/v3/geocode/regeo?key=e384b6b8bc2f8e9e9e92a9cf969da45c&location=113.65004531926762,34.756984446036&output=JSON");

        // 发送请求
        CloseableHttpResponse response = httpClient.execute(httpGet);

        // 获取响应
        String result = EntityUtils.toString(response.getEntity());

        // 解析数据
        JSONObject jsonObject = JSON.parseObject(result);

        JSONObject regeocode = jsonObject.getJSONObject("regeocode");
        JSONObject addressComponent = regeocode.getJSONObject("addressComponent");
        String province = addressComponent.getString("province");
        String city = addressComponent.getString("city");
        String district = addressComponent.getString("district");
        System.out.println(province+","+city+","+district);


        httpClient.close();
    }
}
