package com.hll.util;

import com.hll.udaf.v1.CompareRate;
import org.apache.log4j.Logger;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;

public class FuncUtil {
    public static Logger logger = Logger.getLogger(FuncUtil.class);

    public static Map<String, String> doCompare(Map<String, String> compare, String _type, int whitch, int what) {
        logger.info("compare map " + compare);
        Map<String, String> finalResule = new HashMap<>();
        for (String key : compare.keySet()) {
            Calendar timt = findTime(key);
            String timttype = findTimeType(key);

            double v1 = Double.parseDouble(compare.getOrDefault(key, "0.00"));
            String s = caFormat(timt, timttype);
            logger.info(s + "-" + _type + "-" + whitch);
            String s1 = caFormat(timeSub(timt, _type + "-" + whitch), timttype);
            logger.info("----------- 1 " + s);
            logger.info("----------- 2 " + s1);
            String newKey = key.replaceFirst(s, s1);


            double v2 = Double.parseDouble(compare.getOrDefault(newKey, "0.00"));
            logger.info("compare: key " + key + " v1 " + v1 + " newKey " + newKey + " v2 " + v2);

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

    public static Map<String, String> doRolAccount(Map<String, String> row_account) {
        Map<String, String> finalResule = new HashMap<>();
        double sum_value = 0.00;
        for (String v : row_account.values()) {
            sum_value += Double.parseDouble(v);
        }
        BigDecimal b = new BigDecimal(sum_value);
        for (Map.Entry<String, String> en : row_account.entrySet()) {
            BigDecimal a = new BigDecimal(en.getValue());
            BigDecimal result = a.divide(b, 6, RoundingMode.HALF_UP).setScale(6, BigDecimal.ROUND_UP);
            finalResule.put(en.getKey(), result.toString());
        }
        return finalResule;
    }

    public static Map<String, String> doAddUp(Map<String, String> add_up, String what) {

        Map<String, String> finalResule = new HashMap<>();
        String[] whatArr = what.split("-");
        String dateFormat = whatArr[2];
        int which = Integer.parseInt(whatArr[1]);

        for (String f : add_up.keySet()) {
            final double[] sum = {0.00};
            Calendar now = DateUtil.findTime(f, ",");
            String k1 = DateUtil.formatCalender(now, dateFormat);
            Calendar monthdat;
            switch (which) {
                case 1:
                    monthdat = DateUtil.getFirstDayOfWeek(now);
                    break;
                case 2:
                    monthdat = DateUtil.getFirstDayOfMonth(now);
                    break;
                case 3:
                    monthdat = DateUtil.getFirstDayOfQuarter(now);
                    break;
                case 4:
                    monthdat = DateUtil.getFirstDayOfYear(now);
                    break;
                default: {
                    Calendar ca = Calendar.getInstance();
                    ca.set(Calendar.YEAR, 1970);
                    monthdat = ca;
                }
            }
            Set<String> uniqueValues = new HashSet<>();
            for (String s : DateUtil.getDays(monthdat, now)) {
                String a = DateUtil.formatDateStr(s, dateFormat);
                if (uniqueValues.add(a)) {
                    String tmp = f;
                    sum[0] = sum[0] + Double.parseDouble(add_up.getOrDefault(tmp.replace(k1, a), "0.00"));
                }
            }
            finalResule.put(f, Double.toString(sum[0]));
        }

        return finalResule;
    }


    public static Boolean supportMath(List<String> mathList) {
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
                case "add_up":
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
                case "undo":
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

    public static String getTime(Calendar ca) {
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

    public static Map<String, String> mergerMapV1(Map<String, String> m1, Map<String, String> m2) {
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

    public static Map<String, String> mergerMapV2(Map<String, String> m1, Map<String, String> m2) {
        Map<String, String> result = new HashMap<>();
        if (m1 == null || m1.isEmpty()) {
            return m2;
        }
        if (m2 == null || m2.isEmpty()) {
            return m1;
        }
        Set<String> kset = m1.keySet();
        for (String i : kset) {
            result.put(i, m1.getOrDefault(i, "") + "\001" + m2.getOrDefault(i, ""));
        }
        return result;
    }

    public static Calendar findTime(String key) {
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
                ca.set(Integer.parseInt(day.substring(0, 4)), mtmp - 1, 1);
                break;
            } else if (day.contains("年")) {
                ca.set(Integer.parseInt(day.substring(0, 4)), 0, 1);
                break;
            }

        }
        return ca;
    }

    public static String findTimeType(String key) {
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

    public static Calendar timeSub(Calendar ca, String _type) {
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
        }
        return ca;
    }

    public static String caFormat(Calendar ca, String type_) {
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
