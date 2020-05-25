package com.hll.util;

import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;

public class DateUtil {

    public static List<String> getDays(Calendar start, Calendar end) {
        LinkedList<String> list = new LinkedList<>();
        while (start.before(end) || start.compareTo(end) == 0) {
            list.add(formatCalender(start));
            start.add(Calendar.DAY_OF_YEAR, 1);
        }
        return list;
    }

    public static String formatCalender(Calendar ca) {
        String m = String.valueOf((ca.get(Calendar.MONTH) + 1));
        String d = String.valueOf(ca.get(Calendar.DAY_OF_MONTH));
        if (m.length() < 2) {
            m = "0" + m;
        }
        if (d.length() < 2) {
            d = "0" + d;
        }
        return String.format("%d%s%s", ca.get(Calendar.YEAR), m, d);
    }

    public static String formatDateStr(String day, String format) {
        Calendar ca = Calendar.getInstance();
        ca.set(Integer.parseInt(day.substring(0, 4)),
                Integer.parseInt(day.substring(4, 6)) - 1,
                Integer.parseInt(day.substring(6, 8)));
        return formatCalender(ca, format);
    }

    public static String formatCalender(Calendar ca, String format) {
        switch (format) {
            case "y": {
                return ca.get(Calendar.YEAR) + "年";
            }
            case "yq": {
                int m = ca.get(Calendar.MONTH);
                if (m < 3) {
                    return ca.get(Calendar.YEAR) + "年1季度";
                } else if (m < 6) {
                    return ca.get(Calendar.YEAR) + "年2季度";
                } else if (m < 9) {
                    return ca.get(Calendar.YEAR) + "年3季度";
                } else {
                    return ca.get(Calendar.YEAR) + "年4季度";
                }
            }
            case "ym": {
                String mint = String.valueOf((ca.get(Calendar.MONTH) + 1));
                if (mint.length() < 2) {
                    mint = "0" + mint;
                }
                return ca.get(Calendar.YEAR) + "年" + mint + "月";
            }
            case "yw":
                return ca.get(Calendar.YEAR) + "年" + (ca.get(Calendar.WEEK_OF_YEAR)) + "周";
            case "ymd": {
                String mo = String.valueOf(ca.get(Calendar.MONTH) + 1);
                String d = String.valueOf(ca.get(Calendar.DAY_OF_MONTH));
                if (mo.length() < 2) {
                    mo = "0" + mo;
                }
                if (d.length() < 2) {
                    d = "0" + d;
                }
                return ca.get(Calendar.YEAR) + "年" + mo + "月" + d + "日";
            }
            default:
                throw new RuntimeException("nonexistent dimen_mode. [y,yq,ym,yw,ymd]]");
        }
    }

    public static Calendar createCalender(String dayStr, String _type) {
        Calendar ca = Calendar.getInstance();
        if (_type.equals("ymd")) {
            ca.set(Integer.parseInt(dayStr.substring(0, dayStr.indexOf("年"))),
                    Integer.parseInt(dayStr.substring(dayStr.indexOf("年") + 1, dayStr.indexOf("月"))) - 1,
                    Integer.parseInt(dayStr.substring(dayStr.indexOf("月") + 1, dayStr.indexOf("日"))));
        }

        return ca;
    }

    public static Calendar getFirstDayOfMonth(Calendar ca) {
        Calendar ca2 = Calendar.getInstance();
        ca2.setTime(ca.getTime());
        ca2.set(Calendar.DAY_OF_MONTH, 1);
        return ca2;
    }

    public static Calendar getFirstDayOfYear(Calendar ca) {
        Calendar ca2 = Calendar.getInstance();
        ca2.setTime(ca.getTime());
        ca2.set(Calendar.DAY_OF_MONTH, 1);
        ca2.set(Calendar.MONDAY, 0);
        return ca2;
    }

    public static Calendar getFirstDayOfQuarter(Calendar ca) {
        Calendar ca2 = Calendar.getInstance();
        ca2.setTime(ca.getTime());
        ca2.set(Calendar.DAY_OF_MONTH, 1);
        ca2.set(Calendar.MONDAY, getQuarter(ca2));
        return ca2;
    }

    public static Calendar getFirstDayOfWeek(Calendar ca) {
        Calendar ca2 = Calendar.getInstance();
        ca2.setFirstDayOfWeek(Calendar.MONDAY);
        ca2.setTime(ca.getTime());
        ca2.set(Calendar.DAY_OF_WEEK, ca2.getFirstDayOfWeek());

        return ca2;
    }

    public static int getQuarter(Calendar ca) {
        int month = ca.get(Calendar.MONTH) + 1;
        int quarter = 0;
        if (month <= 3) {
            quarter = 0;
        } else if (month <= 6) {
            quarter = 3;
        } else if (month <= 9) {
            quarter = 6;
        } else {
            quarter = 9;
        }
        return quarter;
    }

    public static Calendar findTime(String key, String delimer) {
        String[] keyArr = key.split(delimer);
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
                int n;
                switch (m) {
                    case 1:
                        n = 1;
                        break;
                    case 2:
                        n = 4;
                        break;
                    case 3:
                        n = 7;
                        break;
                    default:
                        n = 10;
                }
                ca.set(Integer.parseInt(day.substring(0, 4)), n - 1, 1);
                break;
            } else if (day.contains("年")) {
                ca.set(Integer.parseInt(day.substring(0, 4)), 0, 1);
                break;
            }
        }
        return ca;
    }
}
