package com.hll.udaf.v7;

import com.hll.udaf.v1.CompareRate;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hll.util.FuncUtil.*;

@Description(name = "udadvancedcomputing",
        value = "_func_(dimensions, measures, measurefunc) - " +
                " list<string> , list<string> , list<string>")
public class UDAFAdvancedComputing extends UDAF {
    public static Logger logger = Logger.getLogger(CompareRate.class);

    public static class MutableAggregationBuffer {
        private Map<String, Map<String, String>> PartialResult;
    }

    public static class Evaluator implements UDAFEvaluator {
        MutableAggregationBuffer buffer;

        //初始化函数,map和reduce均会执行该函数,起到初始化所需要的变量的作用
        public Evaluator() {
            buffer = new MutableAggregationBuffer();
            init();
        }

        // 初始化函数间传递的中间变量
        public void init() {
            buffer.PartialResult = new HashMap<>();
        }

        //map阶段，返回值为boolean类型，当为true则程序继续执行，当为false则程序退出
        public boolean iterate(List<String> dimensions, List<String> measure, List<String> mathFunction) {

            logger.info(dimensions + "\t" + measure + "\t" + mathFunction);
            StringBuilder sb = new StringBuilder();
            if (dimensions != null && dimensions.size() > 0) {
                for (int i = 0; i < dimensions.size(); i++) {
                    String s = dimensions.get(i);
                    if (i == dimensions.size() - 1) {
                        if (s == null) {
                            sb.append("0");
                        } else {
                            sb.append(s);
                        }
                    } else {
                        if (s == null) {
                            sb.append("0").append("\001");
                        } else {
                            sb.append(s).append("\001");
                        }
                    }
                }
            } else {
                return true;
            }
            String dimenKey = sb.toString().substring(0, sb.length());

            if (!supportMath(mathFunction)) {
                throw new RuntimeException("un support math 【" + mathFunction + "】");
            } else if (measure.size() != mathFunction.size()) {
                throw new RuntimeException("measure.size() != mathFunction.size() 【" + measure.size() + "," + mathFunction.size() + "】");
            }

            Map<String, Map<String, String>> cat = buffer.PartialResult;

            for (int i = 0; i < measure.size(); i++) {
                String value = measure.get(i);
                String name = mathFunction.get(i);

                Map<String, String> inMap = cat.getOrDefault(name, new HashMap<>());
                inMap.put(dimenKey, value);
                cat.put(name, inMap);
            }
            buffer.PartialResult.putAll(cat);
            ////
            String mathFuncStr = mathFunction.stream().reduce((a, b) -> a + "," + b).get();
            Map<String, String> tmp = new HashMap<>();
            tmp.put("mathFuncStr", mathFuncStr);
            buffer.PartialResult.put("mathFuncStr", tmp);

            return true;
        }

        public Map<String, Map<String, String>> terminatePartial() {
            return buffer.PartialResult;
        }

        public boolean merge(Map<String, Map<String, String>> mapOutput) {

            if (mapOutput == null || mapOutput.size() == 0) {
                return true;
            }
            String mathFuncStr = "";
            try {
                mathFuncStr = buffer.PartialResult.get("mathFuncStr").get("mathFuncStr");
                buffer.PartialResult.remove("mathFuncStr");
            } catch (Exception e) {
                System.out.println();
            }

            String mathFuncStr2 = mapOutput.get("mathFuncStr").get("mathFuncStr");
            mapOutput.remove("mathFuncStr");

            Map<String, Map<String, String>> cat1 = buffer.PartialResult;
            Map<String, Map<String, String>> cat2 = mapOutput;
            for (Map.Entry<String, Map<String, String>> en : cat2.entrySet()) {

                String key = en.getKey();
                Map<String, String> value2 = en.getValue();
                Map<String, String> value1 = cat1.getOrDefault(key, new HashMap<>());
                value1.putAll(value2);

                cat1.put(key, value1);
            }
            buffer.PartialResult.putAll(cat1);

            if (mathFuncStr == null || mathFuncStr.length() == 0) {
                mathFuncStr = mathFuncStr2;
            }
            Map<String, String> tmp = new HashMap<>();
            tmp.put("mathFuncStr", mathFuncStr);
            buffer.PartialResult.put("mathFuncStr", tmp);

            return true;
        }

        public Map<String, String> terminate() {

            Map<String, String> finalResule = new HashMap<>();
            Map<String, String> map1 = buffer.PartialResult.get("mathFuncStr");
            if (map1 == null || map1.size() == 0) {
                return finalResule;
            }
            String mathFuncStr = map1.get("mathFuncStr");
            buffer.PartialResult.remove("mathFuncStr");

            Map<String, Map<String, String>> cat = buffer.PartialResult;
            if (cat == null || cat.size() == 0) {
                return finalResule;
            }
            String[] mathFunc = mathFuncStr.split(",");


            for (String whatMath : mathFunc) {
                if (whatMath.startsWith("compare")) {
                    Map<String, String> map = doCompare(cat.getOrDefault(whatMath, new HashMap<>()), whatMath.split("-")[0], Integer.parseInt(whatMath.split("-")[1]), Integer.parseInt(whatMath.split("-")[2]));
                    finalResule = mergerMapV2(finalResule, map);
                } else if (whatMath.startsWith("row_account")) {
                    Map<String, String> map = doRolAccount(cat.getOrDefault(whatMath, new HashMap<>()));
                    finalResule = mergerMapV2(finalResule, map);
                } else if (whatMath.startsWith("add_up")) {
                    Map<String, String> map = doAddUp(cat.getOrDefault(whatMath, new HashMap<>()), whatMath);
                    finalResule = mergerMapV2(finalResule, map);
                } else {
                    finalResule = mergerMapV2(finalResule, cat.get(whatMath));
                }
            }
            return finalResule;
        }

    }
}
