package com.hll.udf.v5;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.log4j.Logger;

import java.util.*;

@Description(name = "udf_adv_row_col_stat",
        value = "_FUNC_(map<string, string> t1, integer rowcol, list<string> row_func, list<string> col_func,\n" +
                "                                 list<string> length) - Returns the mean of a set of numbers")
public class UDFAdvRowColStat extends UDF {
    public static Logger logger = Logger.getLogger(UDFAdvRowColStat.class);

    public List<String> evaluate(Map<String, String> t1, Integer rowcol, List<String> row_func, List<String> col_func,
                                 List<String> length) throws UDFArgumentException {

        logger.info(rowcol);
        logger.info(row_func);
        logger.info(col_func);
        logger.info(length);
        logger.info(t1.size());


        return rowColCompute(t1, rowcol, row_func, col_func, length);

    }

    public List<String> rowColCompute(Map<String, String> t1, Integer rowcol, List<String> row_func, List<String> col_func,
                                      List<String> length) {
        int dimension_length = Integer.parseInt(length.get(0));
        int compare_length = Integer.parseInt(length.get(1));
        int measure_length = Integer.parseInt(length.get(2));

        List<String> resule = new LinkedList<>();

        if (rowcol != 1) {

            StringBuilder mea = new StringBuilder();
            for (int i = 0; i < measure_length; i++) {
                if (i == measure_length - 1) mea.append("0.0");
                else mea.append("0.0\001");
            }
            Map<String, String> totalSumMap = new HashMap<>();
            if (rowcol == 6 || rowcol == 4 || rowcol == 7) {
                String total_key = "总计";
                for (int i = 1; i < dimension_length; i++) {
                    total_key += "\001";
                }
                Map<String, Integer> total_keyCount = new HashMap<>();
                for (Map.Entry<String, String> en : t1.entrySet()) {

                    String key = en.getKey();
                    String[] krr = key.split("\001");
                    String value = en.getValue();
                    String[] vrr = value.split("\001");

                    List<String> compareKey = splitArray(krr, dimension_length, dimension_length + compare_length);
                    String total_pre_key = total_key;
                    if (compareKey != null && !compareKey.isEmpty()) {
                        total_pre_key = String.format("%s\001%s", total_key, mkString(compareKey, "\001"));
                    }
                    List<String> col_measure_func = splitList(col_func, 0, vrr.length);
                    String[] tt = totalSumMap.getOrDefault(total_pre_key, mea.toString()).split("\001");
                    StringBuilder res = new StringBuilder();
                    int count = total_keyCount.getOrDefault(total_pre_key, 0);

                    for (int i = 0; i < vrr.length; i++) {
                        String func = col_measure_func.get(i);
                        double o = Double.parseDouble(tt[i]);
                        double p = 0.0;
                        try {
                            p = Double.parseDouble(vrr[i]);
                        } catch (Exception e) {
                            System.out.println(vrr[i]);
                        }

                        if (func.equals("max-1")) {
                            if (p > o) {
                                o = p;
                            }
                        } else if (func.equals("min-1")) {
                            if (count == 0) {
                                o = 99999999;
                            }
                            if (p < o) {
                                o = p;
                            }
                        } else if (func.equals("sum-1")) {
                            o = o + p;
                        }

                        if (i == vrr.length - 1) {
                            res.append(o);
                        } else {
                            res.append(o).append("\001");
                        }
                    }
                    count += 1;
                    total_keyCount.put(total_pre_key, count);
                    totalSumMap.put(total_pre_key,
                            String.format("%s", res));
                }
                // 总计平均值
                List<String> cf = splitList(col_func, 0, measure_length);
                for (String fun : cf) {
                    if (fun == "avg-1") {
                        for (Map.Entry<String, Integer> en : total_keyCount.entrySet()) {
                            String key = en.getKey();
                            int i = en.getValue();
                            String value = totalSumMap.getOrDefault(key, "0.00\0010.00");
                            String s = Arrays.stream(value.split("\001"))
                                    .map(s1 -> Double.toString((Double.parseDouble(s1) / i)))
                                    .reduce((a, b) -> a + "\001" + b).get();
                            if (totalSumMap.containsKey(key)) {
                                totalSumMap.put(key, s);
                            }
                        }
                    }
                }
            }

            // 分类小计
            Map<String, String> sub_ttal = new HashMap<>();
            Map<String, Integer> subtotal_tmp_keyCount = new HashMap<>();
            if (rowcol == 6 || rowcol == 2 || rowcol == 7) {
                for (Map.Entry<String, String> en : t1.entrySet()) {
                    String k = en.getKey();
                    String v = en.getValue();

                    List<String> dimension_list = splitArray(k.split("\001"), 0, dimension_length);
                    List<String> compare_list = splitArray(k.split("\001"), dimension_length, dimension_length + compare_length);
                    List<String> measure_list = splitArray(v.split("\001"), 0, measure_length);

                    for (int i = 1; i < dimension_length; i++) {

                        String dkey = splitList(dimension_list, 0, i).stream().reduce((a, b) -> a + "\001" + b).get() + "\001小计";
                        if (dkey.split("\001").length < dimension_length) {
                            for (int j = 0; j < (dimension_length - dkey.split("\001").length); j++) {
                                dkey += "\001";
                            }
                        }

                        String subtotal_tmp_key = dkey;
                        if (compare_list != null && !compare_list.isEmpty()) {
                            subtotal_tmp_key = dkey + "\001" + compare_list.stream().reduce((a, b) -> a + "\001" + b).get();
                        }
                        int count = subtotal_tmp_keyCount.getOrDefault(subtotal_tmp_key, 0);
                        String[] tt = sub_ttal.getOrDefault(subtotal_tmp_key, mea.toString()).split("\001");
                        List<String> col_measure_func = splitList(col_func, measure_length, measure_length + 1);
                        String func = col_measure_func.get(0);
                        StringBuilder res = new StringBuilder();
                        for (int j = 0; j < measure_list.size(); j++) {

                            double o = Double.parseDouble(tt[j]);
                            double p = Double.parseDouble(measure_list.get(j));
                            if (func.equals("max-1")) {
                                if (p > o) {
                                    o = p;
                                }
                            } else if (func.equals("min-1")) {
                                if (p < o) {
                                    o = p;
                                }
                            } else if (func.equals("sum-1")) {
                                o = o + p;
                            }

                            if (j == measure_list.size() - 1) {
                                res.append(o);
                            } else {
                                res.append(o).append("\001");
                            }
                        }
                        count += 1;
                        subtotal_tmp_keyCount.put(subtotal_tmp_key, count);
                        sub_ttal.put(subtotal_tmp_key, "%s".format(res.toString()));
                    }
                }
            }

            List<String> a = doRowCount(totalSumMap, dimension_length, compare_length, row_func);
            List<String> b = doRowCount(sub_ttal, dimension_length, compare_length, row_func);
            List<String> c = doRowCount(t1, dimension_length, compare_length, row_func);
            resule.addAll(a);
            resule.addAll(b);
            resule.addAll(c);
        } else {
            resule = doRowCount(t1, 1, 1, row_func);
        }
        return resule;
    }

    public List<String> doRowCount(Map<String, String> m, int dimension_length, int compare_length
            , List<String> row_func) {
        Map<String, Double> hangheji = new HashMap<>();
        Map<String, Double> shuzhixiaoji = new HashMap<>();
        Map<String, Double> fenliexiaoji = new HashMap<>();

        Map<String, Integer> dimenkey2compareSize = new HashMap<>();

        for (Map.Entry<String, String> en :
                m.entrySet()) {
            String[] key = en.getKey().split("\001");
            String[] value = en.getValue().split("\001");

            List<String> dimensionKey = splitArray(key, 0, dimension_length);
            String dKey = mkString(dimensionKey, "\001");
            List<String> compareKey = splitArray(key, dimension_length, dimension_length + compare_length);

            int dimenCompareSize = dimenkey2compareSize.getOrDefault(dKey, 0);
            dimenCompareSize = dimenCompareSize + 1;
            dimenkey2compareSize.put(dKey, dimenCompareSize);

            double hanghejiValue = hangheji.getOrDefault(dKey, 0.00);
            if (hanghejiValue == 0.00 && row_func.get(0).equals("max-1")) hanghejiValue = -9999999;
            else if (hanghejiValue == 0.00 && row_func.get(0).equals("min-1")) hanghejiValue = 9999999;

            double v = Arrays.stream(value).map(s -> {
                try {
                    return Double.parseDouble(s);
                } catch (Exception e) {
                    return 0.00;
                }
            }).reduce(Double::sum).get();
            if (row_func.get(0).equals("max-1")) {
                v = Arrays.stream(value).map(s -> {
                    if (!s.equals("-")) {
                        return Double.parseDouble(s);
                    } else {
                        return 0.0;
                    }
                }).max(Double::compareTo).get();
                if (v > hanghejiValue) {
                    hanghejiValue = v;
                }
            } else if (row_func.get(0).equals("min-1")) {
                v = Arrays.stream(value).map(s -> {
                    if (!s.equals("-")) {
                        return Double.parseDouble(s);
                    } else {
                        return 0.0;
                    }
                }).min(Double::compareTo).get();
                if (v < hanghejiValue) {
                    hanghejiValue = v;
                }
            } else if (row_func.get(0).equals("sum-1") || row_func.get(0).equals("avg-1")) {
                hanghejiValue = hanghejiValue + v;
            }
            hangheji.put(dKey, hanghejiValue);

            // 数值小计
            for (int i = 0; i < value.length; i++) {
                String shuzhikey = dKey + "\001" + i;
                Double shuzhixiaojiValue = shuzhixiaoji.getOrDefault(shuzhikey, 0.00);

                if (shuzhixiaojiValue == 0.00 && row_func.get(i + 1).equals("max-1"))
                    shuzhixiaojiValue = -9999999999999.0;
                else if (shuzhixiaojiValue == 0.00 && row_func.get(i + 1).equals("min-1"))
                    shuzhixiaojiValue = 9999999999.0;
                double v3 = 0;
                if (!value[i].equals("-")) {
                    v3 = Double.parseDouble(value[i]);
                }
                if (row_func.get(i + 1).equals("max-1")) {
                    if (v3 > shuzhixiaojiValue) {
                        shuzhixiaojiValue = v3;
                    }
                } else if (row_func.get(i + 1).equals("min-1")) {

                    if (v3 < shuzhixiaojiValue) {
                        shuzhixiaojiValue = v3;
                    }
                } else if (row_func.get(i + 1).equals("sum-1") || row_func.get(i + 1).equals("avg-1")) {
                    shuzhixiaojiValue = shuzhixiaojiValue + v3;
                }
                shuzhixiaoji.put(shuzhikey, shuzhixiaojiValue);
            }

            // 分列小计
            String fenliekey = dKey + "\001" + mkString(compareKey, "\001");
            double fenliexiaojiValue = fenliexiaoji.getOrDefault(fenliekey, 0.00);
            String fenlieFunc = row_func.get(row_func.size() - 1);
            if (fenliexiaojiValue == 0.00 && fenlieFunc.equals("max-1"))
                fenliexiaojiValue = -9999999;
            else if (fenliexiaojiValue == 0.00 && fenlieFunc.equals("min-1"))
                fenliexiaojiValue = 9999999;

            double v2 = Arrays.stream(value).map(s -> {
                if (!s.equals("-")) {
                    return Double.parseDouble(s);
                } else {
                    return 0.0;
                }
            }).reduce(Double::sum).get();

            if (fenlieFunc.equals("max-1")) {
                v2 = Arrays.stream(value).map(s -> {
                    if (!s.equals("-")) {
                        return Double.parseDouble(s);
                    } else {
                        return 0.0;
                    }
                }).max(Double::compareTo).get();
                if (v2 > fenliexiaojiValue) {
                    fenliexiaojiValue = v2;
                }
            } else if (fenlieFunc.equals("min-1")) {
                v2 = Arrays.stream(value).map(s -> {
                    if (!s.equals("-")) {
                        return Double.parseDouble(s);
                    } else {
                        return 0.0;
                    }
                }).min(Double::compareTo).get();
                if (v2 < fenliexiaojiValue) {
                    fenliexiaojiValue = v2;
                }
            } else if (fenlieFunc.equals("sum-1") || fenlieFunc.equals("avg-1")) {
                fenliexiaojiValue = fenliexiaojiValue + v2;
            }
            fenliexiaoji.put(fenliekey, fenliexiaojiValue);

        }

        logger.info(fenliexiaoji);
        List<String> newResultSeq = new LinkedList<>();
        for (Map.Entry<String, String> en :
                m.entrySet()) {

            String key = en.getKey();
            String value = en.getValue().replace("\001", ",");

            String[] krr = key.split("\001");
            String[] vrr = value.split("\001");
            List<String> dimensionKey = splitArray(krr, 0, dimension_length);
            List<String> compareKey = splitArray(krr, dimension_length, dimension_length + compare_length);
            int size = dimenkey2compareSize.getOrDefault(mkString(dimensionKey, "\001"), 1);

            for (int i = 0; i < vrr.length; i++) {
                double shuzhixiaojiValue = shuzhixiaoji.getOrDefault(mkString(dimensionKey, "\001") + "\001" + i, 0.00);
                if (row_func.get(i + 1).equals("avg-1")) {
                    value = String.format("%s\001%s", value, shuzhixiaojiValue / vrr.length);
                } else {
                    value = String.format("%s\001%s", value, shuzhixiaojiValue);
                }
            }


            double hanghejiValue = hangheji.getOrDefault(mkString(dimensionKey, "\001"), 0.00);
            if (row_func.get(0).equals("avg-1")) {
                value = value + "\001" + hanghejiValue / size;
            } else {
                value = value + "\001" + hanghejiValue;
            }

            String fenliekey = mkString(dimensionKey, "\001") + "\001" + mkString(compareKey, "\001");

            double fenliexiaojiValue = fenliexiaoji.getOrDefault(fenliekey, 0.00);
            if (row_func.get(row_func.size() - 1).equals("avg-1")) {
                value = value + "\001" + fenliexiaojiValue / vrr.length;
            } else {
                value = value + "\001" + fenliexiaojiValue;
            }

            if (key.contains("总计")) {
                value = value + "\001columnSum";
            } else if (key.contains("小计")) {
                value = value + "\001columnSum_subtotal_";
            } else {
                value = value + "\001";
            }


            newResultSeq.add(key + "\001" + value);

        }
        return newResultSeq;

    }

    public static List<String> splitArray(String[] arr, int a, int b) {
        List<String> newArr = new ArrayList<>();
        for (int i = a; i < b; i++) {
            newArr.add(arr[i]);
        }
        return newArr;
    }

    public static List<String> splitList(List<String> arr, int a, int b) {
        List<String> newArr = new ArrayList<>();
        for (int i = a; i < b; i++) {
            newArr.add(arr.get(i));
        }
        return newArr;
    }

    public String mkString(List<String> l, String s) {
        return l.stream().reduce((a, b) -> a + s + b).get();

    }

}
