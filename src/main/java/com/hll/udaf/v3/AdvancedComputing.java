package com.hll.udaf.v3;

import com.hll.udaf.v1.CompareRate;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.log4j.Logger;
import org.omg.PortableInterceptor.INACTIVE;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AdvancedComputing extends UDAF {
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


            String dimenKey = dimensions.stream().reduce((a, b) -> a + "," + b).get();
            if (!supportMath(mathFunction) || (measure.size() != mathFunction.size()))
                throw new RuntimeException("un support math ");
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


            String mathFuncStr = buffer.PartialResult.get("mathFuncStr").get("mathFuncStr");
            buffer.PartialResult.remove("mathFuncStr");

            Map<String, Map<String, String>> cat = buffer.PartialResult;

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

        public Map<String, String> doCompare(Map<String, String> compare, String _type, int whitch, int what) {

            Map<String, String> finalResule = new HashMap<>();
            for (String key : compare.keySet()) {
                Calendar timt = findTime(key);
                String timttype = findTimeType(key);

                double v1 = Double.parseDouble(compare.getOrDefault(key, "0.00"));
                String newKey = key.replaceFirst(caFormat(timt, timttype), caFormat(timeSub(timt, _type + "-" + whitch), timttype));
                double v2 = Double.parseDouble(compare.getOrDefault(newKey, "0.00"));
                if (what == 1) {
                    String r = "-";
                    if (v2 != 0) {
                        r = Double.toString(((v1 - v2) / v2));
                    }
                    finalResule.put(key, r);
                } else if (what == 2) {
                    // 项
                    finalResule.put(key, Double.toString(v2));
                } else {
                    // 值
                    finalResule.put(key, Double.toString((v1 - v2)));
                }
            }
            return finalResule;
        }

        public Boolean supportMath(List<String> mathList) {
            Boolean flag = true;
            for (String math : mathList) {

                String m = math.substring(0, math.indexOf("-"));
                switch (m) {
                    case "compare":
                        flag = true;
                        break;
                    case "row_account":
                        flag = true;
                        break;
                    case "calculate_move":
                        flag = true;
                        break;
                    case "sum":
                        flag = true;
                        break;
                    case "count":
                        flag = true;
                        break;
                    case "max":
                        flag = true;
                        break;
                    case "min":
                        flag = true;
                        break;
                    case "avg":
                        flag = true;
                        break;
                    case "uniqueCount":
                        flag = true;
                        break;
                    case "percentile_25":
                        flag = true;
                        break;
                    case "percentile_75":
                        flag = true;
                        break;
                    case "percentile_50":
                        flag = true;
                        break;
                    default: {
                        flag = false;
                    }
                }
                if (!flag) {
                    break;
                }
            }
            return flag;
        }

        public String getTime(Calendar ca) {
            String m = Integer.toString((ca.get(Calendar.MONTH) + 1));
            String d = Integer.toString(ca.get(Calendar.DAY_OF_MONTH));
            if (m.length() < 2) {
                m = "0" + m;
            }
            if (d.length() < 2) {
                d = "0" + d;
            }

            return ca.get(Calendar.YEAR) + "" + m + "" + d;
        }

        public Map<String, String> mergerMap(Map<String, String> m1, Map<String, String> m2) {
            Map<String, String> result = new HashMap<>();
            if (m1 == null || m1.isEmpty()) {
                return m2;
            }
            if (m2 == null || m2.isEmpty()) {
                return m1;
            }
            Set<String> kset = m1.keySet();
            for (String i : kset) {
                result.put(i, m1.getOrDefault(i, "") + "::" + m2.getOrDefault(i, ""));
            }
            return result;
        }


        public Calendar findTime(String key) {
            String[] keyArr = key.split("_");
            Calendar ca = Calendar.getInstance();
            for (String day : keyArr) {

                if (day.contains("年") && day.contains("月") && day.contains("日")) {
                    ca.set(Integer.parseInt(day.substring(0, 4)),
                            Integer.parseInt(day.substring(day.indexOf("年") + 1, day.indexOf("月"))) - 1,
                            Integer.parseInt(day.substring(day.indexOf("月") + 1, day.indexOf("日"))));
                    break;
                } else if (day.contains("年") && day.contains("月")) {
                    ca.set(Integer.parseInt(day.substring(0, 4)),
                            Integer.parseInt(day.substring(day.indexOf("年") + 1, day.indexOf("月"))) - 1,
                            1);
                    break;
                } else if (day.contains("年") && day.contains("周")) {
                    ca.setFirstDayOfWeek(Calendar.MONDAY);
                    ca.set(Calendar.YEAR, Integer.parseInt(day.substring(0, 4)));
                    ca.set(Calendar.WEEK_OF_YEAR, Integer.parseInt(day.substring(day.indexOf("年") + 1, day.indexOf("周"))));
                    break;
                } else if (day.contains("年") && day.contains("季度")) {
                    int m = Integer.parseInt(day.substring(day.indexOf("年") + 1, day.indexOf("季度")));
                    int mtmp = 1;
                    if (m == 1) {
                        mtmp = 1;
                    } else if (m == 2) {
                        mtmp = 4;
                    } else if (m == 3) {
                        mtmp = 7;
                    } else if (m == 4) {
                        mtmp = 10;
                    }
                    ca.set(Integer.parseInt(day.substring(0, 4)), m - 1, 1);
                    break;
                } else if (day.contains("年")) {
                    ca.set(Integer.parseInt(day.substring(0, 4)), 0, 1);
                    break;
                }

            }
            return ca;
        }

        public String findTimeType(String key) {
            String[] keyArr = key.split("_");
            String ca = "ymd";
            for (String day : keyArr) {

                if (day.contains("年") && day.contains("月") && day.contains("日")) {
                    ca = "ymd";
                    break;
                } else if (day.contains("年") && day.contains("月")) {
                    ca = "ym";
                    break;
                } else if (day.contains("年") && day.contains("周")) {
                    ca = "yw";
                    break;
                } else if (day.contains("年") && day.contains("季度")) {
                    ca = "yq";
                    break;
                } else if (day.contains("年")) {
                    ca = "y";
                    break;
                }

            }
            return ca;
        }

        public Calendar timeSub(Calendar ca, String _type) {
            if (_type.startsWith("compare-1")) {
                ca.add(Calendar.DAY_OF_YEAR, -1);
            } else if (_type.startsWith("compare-2")) {
                ca.add(Calendar.WEEK_OF_YEAR, -1);
            } else if (_type.startsWith("compare-3")) {
                ca.add(Calendar.MONTH, -1);
            } else if (_type.startsWith("compare-4")) {
                ca.add(Calendar.YEAR, -1);
            } else if (_type.startsWith("compare-5")) {
                ca.add(Calendar.MONTH, -3);
            } else if (_type.startsWith("compare_rate-1")) {
                ca.add(Calendar.DAY_OF_YEAR, -1);
            } else if (_type.startsWith("compare_rate-2")) {
                ca.add(Calendar.WEEK_OF_YEAR, -1);
            } else if (_type.startsWith("compare_rate-3")) {
                ca.add(Calendar.MONTH, -1);
            } else if (_type.startsWith("compare_rate-4")) {
                ca.add(Calendar.YEAR, -1);
            } else if (_type.startsWith("compare_rate-5")) {
                ca.add(Calendar.MONTH, -3);
            }
            return ca;
        }

        public String caFormat(Calendar ca, String type_) {
            switch (type_) {
                case "y": {
                    return ca.get(Calendar.YEAR) + "年";
                }
                case "yq": {
                    int m = ca.get(Calendar.MONTH);
                    if (m >= 0 && m < 3) {
                        return ca.get(Calendar.YEAR) + "年1季度";
                    } else if (m >= 3 && m < 6) {
                        return ca.get(Calendar.YEAR) + "年2季度";
                    } else if (m >= 6 && m < 9) {
                        return ca.get(Calendar.YEAR) + "年3季度";
                    } else {
                        return ca.get(Calendar.YEAR) + "年4季度";
                    }
                }
                case "ym": {
                    String mint = Integer.toString((ca.get(Calendar.MONTH) + 1));
                    if (mint.length() < 2) {
                        mint = "0" + mint;
                    }
                    return ca.get(Calendar.YEAR) + "年" + mint + "月";
                }
                case "yw": {
                    return ca.get(Calendar.YEAR) + "年" + (ca.get(Calendar.WEEK_OF_YEAR)) + "周";
                }
                case "ymd": {
                    String m = Integer.toString((ca.get(Calendar.MONTH) + 1));
                    String d = Integer.toString(ca.get(Calendar.DAY_OF_MONTH));
                    if (m.length() < 2) {
                        m = "0" + m;
                    }
                    if (d.length() < 2) {
                        d = "0" + d;
                    }
                    return ca.get(Calendar.YEAR) + "年" + m + "月" + d + "日";
                }
                default:
                    throw new RuntimeException("nonexistent dimen_mode. [y,yq,ym,yw,ymd]]");
            }
        }
    }
}
