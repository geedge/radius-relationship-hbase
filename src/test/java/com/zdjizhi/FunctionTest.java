package com.zdjizhi;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * @author qidaijie
 * @Package com.zdjizhi
 * @Description:
 * @date 2021/6/2314:32
 */
public class FunctionTest {

    @Test
    public void jsonTest() {
        Map<String, Long> methodCount = new HashMap<>(16);
        methodCount.put("A",20L);
        methodCount.put("B",20L);
        methodCount.put("C",20L);
        String jsonString = JSON.toJSONString(methodCount);
        System.out.println(jsonString);
        JSONObject jsonObject = JSONObject.parseObject(jsonString);
        Map<String, Object> hmCount = (Map<String, Object>) jsonObject;
        System.out.println(hmCount.toString());


    }


}
