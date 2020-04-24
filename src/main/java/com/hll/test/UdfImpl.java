package com.hll.test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UdfImpl implements Udf {


    @Override
    public boolean iterate(Map<String, Map<String, String>> PartialResult, List<String> dimensions, List<String> measure, List<String> mathFunction) {

        String dimenKey = dimensions.stream().reduce((a, b) -> a + "," + b).get();
        Map<String, Map<String, String>> cat = PartialResult;

        for (int i = 0; i < measure.size(); i++) {
            String value = measure.get(i);
            String name = mathFunction.get(i);

            Map<String, String> inMap = cat.getOrDefault(name, new HashMap<>());
            inMap.put(dimenKey, value);
            cat.put(name, inMap);
        }
        PartialResult.putAll(cat);
        ////
        String mathFuncStr = mathFunction.stream().reduce((a, b) -> a + "," + b).get();
        Map<String, String> tmp = new HashMap<>();
        tmp.put("mathFuncStr", mathFuncStr);
        PartialResult.put("mathFuncStr", tmp);

        return true;
    }

    @Override
    public Map<String, Map<String, String>> terminatePartial() {
        return null;
    }

    @Override
    public boolean merge(Map<String, Map<String, String>> PartialResult, Map<String, Map<String, String>> mapOutput) {
        return false;
    }

    @Override
    public Map<String, String> terminate() {
        return null;
    }
}
