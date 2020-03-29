package com.hll.udaf;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.log4j.Logger;
import org.stringtemplate.v4.ST;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Description(name = "compare", value = "_FUNC_(dimension,measure,dimen_mode,time_diff_type,measure_name) - Returns the mean of a set of numbers")

public class RowColStat extends UDAF {

    public static Logger logger = Logger.getLogger(RowColStat.class);

    public static class ArgsState {
        private Map<String, Map<String, String>> cat;
        private Map<String, String> dog;
        private Map<String, String> info;
    }

    public static class Evaluator implements UDAFEvaluator {
        ArgsState argsState;

        //初始化函数,map和reduce均会执行该函数,起到初始化所需要的变量的作用
        public Evaluator() {
            argsState = new ArgsState();
            init();
        }

        // 初始化函数间传递的中间变量
        public void init() {
            argsState.cat = new HashMap<>();
            argsState.dog = new HashMap<>();
            // measure_func
            // rowcol
            // dimension_length
            // compare_length
            // measure_length
            // rowsumtype
            argsState.info = new HashMap<>();
        }

        //map阶段，返回值为boolean类型，当为true则程序继续执行，当为false则程序退出
        public boolean iterate(List<String> input0, List<String> compare, List<String> measure_value,
                               String rowcol_num, String dimen_mode, List<String> measure_func, List<String> rowsumtype) {

            // 行统计维度 cat
            // 时间格式化 ymd ...
            argsState.info.put("rowcol", rowcol_num);
            // dimensions
            // 行键
            List<String> dimensions = input0.stream().map(new Function<String, String>() {
                @Override
                public String apply(String d) {
                    return hasRD(d) > 7 ? dayformat(d, dimen_mode, hasRD(d)) : d;
                }
            }).collect(Collectors.toList());
            // dimension_length
            argsState.info.put("dimension_length", Integer.toString(dimensions.size()));

            String dimension_key = dimensions.stream().reduce((a, b) -> String.format("%s%s", a, b)).get();
            String compare_key = compare.stream().reduce((a, b) -> String.format("%s%s", a, b)).get();
            // compare_length
            argsState.info.put("compare_length", Integer.toString(compare.size()));
            // measure_length
            argsState.info.put("measure_length", Integer.toString(measure_value.size()));
            // rowsumtype
            String rowsum_type = rowsumtype.stream().reduce((a, b) -> String.format("%s_%s", a, b)).get();
            argsState.info.put("rowsumtype", rowsum_type);

            //
            argsState.info.put("measure_func", measure_func.stream().reduce((a, b) -> String.format("%s_%s", a, b)).get());

            Map<String, String> measure = new HashMap<>();
            for (int i = 0; i < measure_func.size(); i++) {
                measure.put(measure_func.get(i), measure_value.get(i));
            }

            final Map<String, String>[] subCat = new Map[]{argsState.cat.getOrDefault(dimension_key, new HashMap<String, String>())};

            measure.entrySet().forEach(mea -> {
                String key = mea.getKey();
                String value = mea.getValue();
                String[] key_arr = key.split("-");

                String mea_key = compare_key + "△" + key;

                if (key_arr[1].equals("sum")) {
                    double v = Double.parseDouble(subCat[0].getOrDefault(mea_key, "0.00"));
                    v = (v + Double.parseDouble(value));
                    subCat[0].put(mea_key, Double.toString(v));

                } else if (key_arr[1].equals("count")) {
                    double v = Double.parseDouble(subCat[0].getOrDefault(mea_key, "0.00"));
                    v = (v + 1);
                    subCat[0].put(mea_key, Double.toString(v));
                } else if (key_arr[1].equals("max")) {
                    double v = Double.parseDouble(subCat[0].getOrDefault(mea_key, "0.00"));
                    if (Double.parseDouble(value) > v) {
                        v = Double.parseDouble(value);
                    }
                    subCat[0].put(mea_key, Double.toString(v));
                } else if (key_arr[1].equals("min")) {
                    double v = Double.parseDouble(subCat[0].getOrDefault(mea_key, "0.00"));
                    if (Double.parseDouble(value) < v) {
                        v = Double.parseDouble(value);
                    }
                    subCat[0].put(mea_key, Double.toString(v));
                } else if (key_arr[1].equals("avg")) {
                    String v = subCat[0].getOrDefault(mea_key, "0,0");
                    double a = Double.parseDouble(v.split(",")[0]);
                    double b = Double.parseDouble(v.split(",")[1]) + 1.0;
                    a = a + Double.parseDouble(value);
                    subCat[0].put(mea_key, String.format("%s,%s", a, b));
                } else {
                    String v = subCat[0].getOrDefault(mea_key, "");
                    if (v.equals("")) {
                        v = value;
                    } else if (!v.contains(value)) {
                        v = String.format("%s,%s", v, value);
                    }
                    subCat[0].put(mea_key, v);
                }


            });
            argsState.cat.put(dimension_key, subCat[0]);

            // 行统计
            for (Map.Entry<String, String> en : measure.entrySet()) {
                String key = en.getKey();

                String dimension_tmp_key = String.format("%s△%s", dimension_key, key);
                String m_value = en.getValue();

                if (key.endsWith("sum")) {

                    double v = Double.parseDouble(argsState.dog.getOrDefault(dimension_tmp_key, "0.00"));
                    v = v + Double.parseDouble(m_value);
                    argsState.dog.put(dimension_tmp_key, Double.toString(v));
                } else if (key.endsWith("count")) {

                    double v = Double.parseDouble(argsState.dog.getOrDefault(dimension_tmp_key, "0.00"));
                    v = v + 1;
                    argsState.dog.put(dimension_tmp_key, Double.toString(v));
                } else if (key.endsWith("max")) {

                    double v = Double.parseDouble(argsState.dog.getOrDefault(dimension_tmp_key, "0.00"));
                    if (Double.parseDouble(m_value) > v) {
                        v = Double.parseDouble(m_value);
                    }
                    argsState.dog.put(dimension_tmp_key, Double.toString(v));
                } else if (key.endsWith("min")) {

                    double v = Double.parseDouble(argsState.dog.getOrDefault(dimension_tmp_key, "0.00"));
                    if (Double.parseDouble(m_value) < v) {
                        v = Double.parseDouble(m_value);
                    }
                    argsState.dog.put(dimension_tmp_key, Double.toString(v));
                } else if (key.endsWith("avg")) {

                    String v = argsState.dog.getOrDefault(dimension_tmp_key, "0,0");
                    double a = Double.parseDouble(v.split(",")[0]);
                    double b = Double.parseDouble(v.split(",")[1]) + 1.0;
                    a = a + Double.parseDouble(m_value);
                    argsState.dog.put(dimension_tmp_key, String.format("%s,%s", a, b));
                } else {
                    String v = argsState.dog.getOrDefault(dimension_tmp_key, "");
                    if (v.equals("")) {
                        v = m_value;
                    } else if (!v.contains(m_value)) {
                        v = String.format("%s,%s", v, m_value);
                    }
                    argsState.dog.put(dimension_tmp_key, v);
                }
            }


            return true;
        }

        /**
         * 类似于combiner,在map范围内做部分聚合，将结果传给merge函数中的形参mapOutput
         * 如果需要聚合，则对iterator返回的结果处理，否则直接返回iterator的结果即可
         */
        public Map<String, Map<String, String>> terminatePartial() {
            argsState.cat.put("dog", argsState.dog);
            argsState.cat.put("info", argsState.info);

            return argsState.cat;
        }

        // reduce 阶段，用于逐个迭代处理map当中每个不同key对应的 terminatePartial的结果
        public boolean merge(Map<String, Map<String, String>> mapOutput) {

            Map<String, String> dog = mapOutput.getOrDefault("dog", new HashMap<>());
            if (dog == null || dog.size() == 0) {
                throw new RuntimeException("dog is null");
            }
            argsState.dog = dog;

            Map<String, String> info = mapOutput.getOrDefault("info", new HashMap<>());
            if (info == null || info.size() == 0) {
                throw new RuntimeException("info is null");
            }
            argsState.info = info;
            mapOutput.remove("dog");
            mapOutput.remove("info");

            argsState.cat = mapOutput;
            if (argsState.cat == null || argsState.cat.size() == 0) {
                throw new RuntimeException("cat is null");
            }
            return true;
        }

        // 处理merge计算完成后的结果，即对merge完成后的结果做最后的业务处理
        public Map<String, String> terminate() {
            return null;
        }
    }

    public static String getTime(Calendar ca) {
        String m = Integer.toString(ca.get(Calendar.MONTH) + 1);
        String d = Integer.toString(ca.get(Calendar.DAY_OF_MONTH));
        if (m.length() < 2) {
            m = String.format("0%s", m);
        }
        if (d.length() < 2) {
            d = String.format("0%s", d);
        }

        return String.format("%d%s%s", ca.get(Calendar.YEAR), m, d);
    }

    public static String dayformat(String day, String dimen_mode, int format) {
        if (day.length() != 8 && day.length() != 19 && day.length() != 10) {
            return day;
        }
        Calendar ca = Calendar.getInstance();
        if (format == 19 || format == 10) {
            ca.set(Integer.parseInt(day.substring(0, 4)), Integer.parseInt(day.substring(5, 7)) - 1, Integer.parseInt(day.substring(8, 10)));
        } else if (format == 8) {
            ca.set(Integer.parseInt(day.substring(0, 4)), Integer.parseInt(day.substring(4, 6)) - 1, Integer.parseInt(day.substring(6, 8)));
        } else {
            return day;
        }

        if (dimen_mode.equals("y")) {
            return ca.get(Calendar.YEAR) + "年";
        } else if (dimen_mode.equals("yq")) {
            int m = ca.get(Calendar.MONTH);
            if (m >= 0 && m < 3) {
                return String.format("%d年1季度", ca.get(Calendar.YEAR));
            } else if (m >= 3 && m < 6) {
                return String.format("%d年2季度", ca.get(Calendar.YEAR));
            } else if (m >= 6 && m < 9) {
                return String.format("%d年3季度", ca.get(Calendar.YEAR));
            } else {
                return String.format("%d年4季度", ca.get(Calendar.YEAR));
            }
        } else if (dimen_mode.equals("ym")) {
            String mint = Integer.toString(ca.get(Calendar.MONTH) + 1);
            if (mint.length() < 2) {
                mint = String.format("0%s", mint);
            }

            return String.format("%d年%s月", ca.get(Calendar.YEAR), mint);
        } else if (dimen_mode.equals("yw")) {
            return String.format("%d年%d周", ca.get(Calendar.YEAR), ca.get(Calendar.WEEK_OF_YEAR));
        } else {
            String m = Integer.toString(ca.get(Calendar.MONTH) + 1);
            String d = Integer.toString(ca.get(Calendar.DAY_OF_MONTH));
            if (m.length() < 2) {
                m = String.format("0%s", m);
            }
            if (d.length() < 2) {
                d = String.format("0%s", d);
            }
            return String.format("%d年%s月%s日", ca.get(Calendar.YEAR), m, d);
        }
    }

    public static int hasRD(String input) {
        String s = "\\d+-\\d+-\\d+ \\d+:\\d+:\\d+";
        Pattern pattern = Pattern.compile(s);
        Matcher ma = pattern.matcher(input);
        if (ma.find()) return 19;
        else {
            s = "\\d+-\\d+-\\d+";
            pattern = Pattern.compile(s);
            ma = pattern.matcher(input);
            if (ma.find()) return 10;
            else {
                s = "^[0-9]*$";
                pattern = Pattern.compile(s);
                ma = pattern.matcher(input);
                if (ma.find()) return 8;
                else return 0;
            }
        }
    }

    public static List<String> splitArray(List<String> arr, int a, int b) {
        List<String> newArr = new ArrayList<>();
        for (int i = a; i < b; i++) {
            newArr.add(arr.get(i));
        }
        return newArr;
    }

}
