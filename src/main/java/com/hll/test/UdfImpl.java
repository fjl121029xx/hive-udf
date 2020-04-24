package com.hll.test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hll.util.FuncUtil.doCompare;
import static com.hll.util.FuncUtil.mergerMap;

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

        if (mapOutput == null || mapOutput.size() == 0) {
            return true;
        }
        String mathFuncStr = "";
        if (PartialResult != null && PartialResult.containsKey("mathFuncStr")) {
            mathFuncStr = PartialResult.get("mathFuncStr").get("mathFuncStr");
            PartialResult.remove("mathFuncStr");
        }
        String mathFuncStr2 = mapOutput.get("mathFuncStr").get("mathFuncStr");
        mapOutput.remove("mathFuncStr");

        Map<String, Map<String, String>> cat1 = PartialResult;
        Map<String, Map<String, String>> cat2 = mapOutput;
        for (Map.Entry<String, Map<String, String>> en : cat2.entrySet()) {

            String key = en.getKey();
            Map<String, String> value2 = en.getValue();
            Map<String, String> value1 = cat1.getOrDefault(key, new HashMap<>());
            value1.putAll(value2);

            cat1.put(key, value1);
        }
        PartialResult.putAll(cat1);

        if (mathFuncStr == null || mathFuncStr.length() == 0) {
            mathFuncStr = mathFuncStr2;
        }
        Map<String, String> tmp = new HashMap<>();
        tmp.put("mathFuncStr", mathFuncStr);
        PartialResult.put("mathFuncStr", tmp);

        return true;
    }

    @Override
    public Map<String, String> terminate(Map<String, Map<String, String>> PartialResult) {

        String mathFuncStr = PartialResult.get("mathFuncStr").get("mathFuncStr");
        PartialResult.remove("mathFuncStr");

        Map<String, Map<String, String>> cat = PartialResult;

        String[] mathFunc = mathFuncStr.split(",");

        Map<String, String> finalResule = new HashMap<>();
        for (String whatMath : mathFunc) {
            if (whatMath.startsWith("compare")) {
                Map<String, String> map = doCompare(cat.getOrDefault(whatMath, new HashMap<>()), whatMath.split("-")[0], Integer.parseInt(whatMath.split("-")[1]), Integer.parseInt(whatMath.split("-")[2]));
                finalResule = mergerMap(finalResule, map);
            } else {
                finalResule = mergerMap(finalResule, cat.get(whatMath));
            }
        }
        return finalResule;
    }
}
