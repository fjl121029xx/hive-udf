package com.hll.udf.v4;

import com.hll.udaf.v6.RowColStatisticsV3;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.log4j.Logger;

import java.sql.Timestamp;
import java.util.*;

import static com.sun.org.apache.xalan.internal.xsltc.compiler.util.Type.Int;

@Description(name = "udf_row_col_stst",
        value = "_FUNC_(map<string, string> t1, integer rowcol, list<string> row_func, list<string> col_func,\n" +
                "                                 list<string> length) - Returns the mean of a set of numbers")
public class UDFRowColStat extends UDF {
    public static Logger logger = Logger.getLogger(UDFRowColStat.class);

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
                else mea.append("0.0::");
            }
            Map<String, String> totalSumMap = new HashMap<>();
            if (rowcol == 6 || rowcol == 4 || rowcol == 7) {
                String total_key = "总计";
                for (int i = 1; i < dimension_length; i++) {
                    total_key += ",";
                }
                Map<String, Integer> total_keyCount = new HashMap<>();
                for (Map.Entry<String, String> en : t1.entrySet()) {

                    String key = en.getKey();
                    String[] krr = key.split(",");
                    String value = en.getValue();
                    String[] vrr = value.split("::");

                    List<String> compareKey = splitArray(krr, dimension_length, dimension_length + compare_length);
                    String total_pre_key = total_key;
                    if (compareKey != null && !compareKey.isEmpty()) {
                        total_pre_key = String.format("%s,%s", total_key, mkString(compareKey, ","));
                    }
                    List<String> col_measure_func = splitList(col_func, 0, vrr.length);
                    String[] tt = totalSumMap.getOrDefault(total_pre_key, mea.toString()).split("::");
                    StringBuilder res = new StringBuilder();
                    int count = total_keyCount.getOrDefault(total_pre_key, 0);

                    for (int i = 0; i < vrr.length; i++) {
                        String func = col_measure_func.get(i);
                        double o = Double.parseDouble(tt[i]);
                        double p = Double.parseDouble(vrr[i]);
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
                            res.append(o).append("::");
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
                            String value = totalSumMap.getOrDefault(key, "0.00::0.00");
                            String s = Arrays.stream(value.split("::"))
                                    .map(s1 -> Double.toString((Double.parseDouble(s1) / i)))
                                    .reduce((a, b) -> a + "::" + b).get();
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
                    String v = en.getKey();
                    List<String> dimension_list = splitArray(k.split(","), 0, dimension_length);
                    List<String> compare_list = splitArray(k.split(","), dimension_length, dimension_length + compare_length);
                    List<String> measure_list = splitArray(v.split("::"), 0, measure_length);

                    for (int i = 1; i < dimension_length; i++) {

                        String dkey = splitList(dimension_list, 0, i).stream().reduce((a, b) -> a + "," + b).get() + ",小计";
                        if (dkey.split(",").length < dimension_length) {
                            for (int j = 0; j < (dimension_length - dkey.split(",").length); j++) {
                                dkey += ",";
                            }
                        }

                        String subtotal_tmp_key = dkey;
                        if (compare_list != null && !compare_list.isEmpty()) {
                            subtotal_tmp_key = dkey + "," + compare_list.stream().reduce((a, b) -> a + "," + b).get();
                        }
                        int count = subtotal_tmp_keyCount.getOrDefault(subtotal_tmp_key, 0);
                        String[] tt = sub_ttal.getOrDefault(subtotal_tmp_key, mea.toString()).split("::");
                        List<String> col_measure_func = splitList(col_func, measure_length, measure_length + 1);
                        String func = col_measure_func.get(0);
                        StringBuilder res = new StringBuilder();
                        for (int j = 0; j < measure_list.size(); j++) {

                            double o = Double.parseDouble(tt[i]);
                            double p = Double.parseDouble(measure_list.get(i));
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
                                res.append(o).append("::");
                            }
                        }
                        count += 1;
                        subtotal_tmp_keyCount.put(subtotal_tmp_key, count);
                        sub_ttal.put(subtotal_tmp_key, "%s".format(res.toString()));
                    }
                }
            }
            totalSumMap.putAll(sub_ttal);
            totalSumMap.putAll(t1);
            resule = doRowCount(totalSumMap, 1, 1);
        } else {
            resule = doRowCount(t1, 1, 1);
        }
        return resule;
    }

    public List<String> doRowCount(Map<String, String> m, int dimension_length, int compare_length) {
        Map<String, Double> hangheji = new HashMap<>();
        Map<String, Double> shuzhixiaoji = new HashMap<>();
        Map<String, Double> fenliexiaoji = new HashMap<>();

        for (Map.Entry<String, String> en :
                m.entrySet()) {
            String[] key = en.getKey().split(",");
            String[] value = en.getValue().split("::");
            List<String> dimensionKey = splitArray(key, 0, dimension_length);
            List<String> compareKey = splitArray(key, dimension_length, dimension_length + compare_length);
            Double v = Arrays.stream(value).map(Double::parseDouble).reduce(Double::sum).get();
            String dKey = mkString(dimensionKey, "△");
            hangheji.put(dKey, (v + hangheji.getOrDefault(dKey, 0.00)));

            // 数值小计
            for (int i = 0; i < value.length; i++) {
                String shuzhikey = dKey + "△" + i;
                shuzhixiaoji.put(shuzhikey, (Double.parseDouble(value[i]) + shuzhixiaoji.getOrDefault(shuzhikey, 0.00)));

            }
            // 分列小计
            String fenliekey = dKey + "△" + mkString(compareKey, "△");
            fenliexiaoji.put(fenliekey, (v + fenliexiaoji.getOrDefault(fenliekey, 0.00)));

        }
        List<String> newResultSeq = new LinkedList<>();
        for (Map.Entry<String, String> en :
                m.entrySet()) {

            String key = en.getKey();
            String value = en.getValue();

            String[] krr = key.split(",");
            String[] vrr = key.split("::");
            List<String> dimensionKey = splitArray(krr, 0, dimension_length);
            List<String> compareKey = splitArray(krr, dimension_length, dimension_length + compare_length);

            for (int i = 0; i < vrr.length; i++) {
                double shuzhixiaojiValue = shuzhixiaoji.getOrDefault(mkString(dimensionKey, "△") + "△" + i, 0.00);
                value = value + "::" + shuzhixiaojiValue;
            }

            double fenliexiaojiValue = fenliexiaoji.getOrDefault(mkString(dimensionKey, "△") + "△" + mkString(compareKey, "△"), 0.00);
            value = value + "::" + fenliexiaojiValue;
            double hanghejiValue = hangheji.getOrDefault(mkString(dimensionKey, "△"), 0.00);
            value = value + "::" + hanghejiValue;

            if (key.contains("总计")) {
                value = value + "::columnSum";
            } else if (key.contains("小计")) {
                value = value + "::columnSum_subtotal_";
            } else {
                value = value + "::";
            }
            newResultSeq.add(key + ":" + value);

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
