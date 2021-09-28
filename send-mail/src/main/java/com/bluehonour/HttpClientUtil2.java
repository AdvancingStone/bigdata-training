package com.bluehonour;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;

public class HttpClientUtil2 {
    public static void main(String[] args) {
        //建立一个新的请求客户端
        CloseableHttpClient httpClient = HttpClients.createDefault();

        //使用HttpGet方式请求网址
        HttpGet httpGet = new HttpGet("https://hitokoto.cn/");

        //获取网址的返回结果
        CloseableHttpResponse response = null;
        try {
            response = httpClient.execute(httpGet);
        } catch (IOException e) {
            e.printStackTrace();
        }

        //获取返回结果中的实体
        HttpEntity entity = response.getEntity();

        //将返回的实体输出
        try {
            System.out.println(EntityUtils.toString(entity));
            EntityUtils.consume(entity);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
