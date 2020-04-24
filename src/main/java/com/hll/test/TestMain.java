package com.hll.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class TestMain {

    public static void main(String[] args) {
        Map<String, Map<String, String>> PartialResult = new HashMap<>();

        Udf avc = new UdfImpl();

        Udf proxy_adv = new ProxyUDF(avc);
        proxy_adv.iterate(PartialResult, new ArrayList<>(), new ArrayList<>(), new ArrayList<>());

        System.out.println(PartialResult);
    }
}
