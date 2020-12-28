package com.bluehonour.json;

import com.alibaba.fastjson.JSON;
import org.junit.Test;

import java.util.List;

public class FASTJson {

    //解析
    @Test
    public void test1() {
        // 对象嵌套数组嵌套对象
        String json1 = "{'id':1,'name':'JAVAEE-1703','stus':[{'id':101,'name':'刘铭','age':16}]}";
        // 数组
        String json2 = "['北京','天津','杭州']";
        //1、
        //静态方法
        String grade= JSON.parseObject(json1, String.class);
        System.out.println(grade);
        //2、
        List<String> list=JSON.parseArray(json2, String.class);
        System.out.println(list);
    }

}