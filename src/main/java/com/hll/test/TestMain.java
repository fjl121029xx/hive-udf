package com.hll.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class TestMain {

    public static void main(String[] args) {
        Map<String, Map<String, String>> PartialResult = new HashMap<>();

        Udf avc = new UdfImpl();

        Udf proxy_adv = new ProxyUDF(avc);
        proxy_adv.iterate(PartialResult, new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
        Map<String, Map<String, String>> mergeResult = new HashMap<>();
        proxy_adv.merge(mergeResult, PartialResult);

//        System.out.println(PartialResult);
        Map<String, String> result = proxy_adv.terminate(mergeResult);
//        System.out.println(result);
        String resuleJson = JSON.toJSONString(result);
        System.out.println(resuleJson);
    }
}
