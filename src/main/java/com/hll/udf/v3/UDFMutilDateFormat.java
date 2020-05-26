package com.hll.udf.v3;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.log4j.Logger;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Description(name = "udfmutildateformat",
        value = "_FUNC_(date, format) - " +
                "date:String [20200316/2020-03-16/2020-03-16 18:11:39] , day options[y,yq,ym,yw,ymd,yh]")
public class UDFMutilDateFormat extends GenericUDF {

    public static Logger logger = Logger.getLogger(UDFMutilDateFormat.class);

    private TimestampObjectInspector timestampObjectInspector01;
    private JavaLongObjectInspector longObjectInspector01;
    private JavaStringObjectInspector stringObjectInspector01;
    private JavaDateObjectInspector dateObjectInspector01;

    private WritableConstantStringObjectInspector stringObjectInspector02;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length != 2) {
            throw new UDFArgumentLengthException("arrayContainsExample only takes 2 arguments: String,  String");
        }

        ObjectInspector a = objectInspectors[0];
        ObjectInspector b = objectInspectors[1];


        if (a instanceof TimestampObjectInspector) {
            this.timestampObjectInspector01 = (TimestampObjectInspector) a;
            this.stringObjectInspector02 = (WritableConstantStringObjectInspector) b;
        } else if (a instanceof JavaLongObjectInspector) {
            this.longObjectInspector01 = (JavaLongObjectInspector) a;
            this.stringObjectInspector02 = (WritableConstantStringObjectInspector) b;
        } else if (a instanceof JavaStringObjectInspector) {
            this.stringObjectInspector01 = (JavaStringObjectInspector) a;
            this.stringObjectInspector02 = (WritableConstantStringObjectInspector) b;
        } else if (a instanceof JavaDateObjectInspector) {
            this.dateObjectInspector01 = (JavaDateObjectInspector) a;
            this.stringObjectInspector02 = (WritableConstantStringObjectInspector) b;
        } else {
            throw new UDFArgumentException(String.format("wrong type %s %s", a.getClass(), b.getClass()));
        }

        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    public Calendar ChineseDateFormat(String day) {
        Calendar ca = Calendar.getInstance();
        if (day.contains("年") && day.contains("月") && day.contains("日")) {
            ca.set(Integer.parseInt(day.substring(0, 4)),
                    Integer.parseInt(day.substring(day.indexOf("年") + 1, day.indexOf("月"))) - 1,
                    Integer.parseInt(day.substring(day.indexOf("月") + 1, day.indexOf("日"))));
        } else if (day.contains("年") && day.contains("月")) {
            ca.set(Integer.parseInt(day.substring(0, 4)),
                    Integer.parseInt(day.substring(day.indexOf("年") + 1, day.indexOf("月"))) - 1,
                    1);
        } else if (day.contains("年") && day.contains("周")) {
            ca.setFirstDayOfWeek(Calendar.MONDAY);
            ca.set(Calendar.YEAR, Integer.parseInt(day.substring(0, 4)));
            ca.set(Calendar.WEEK_OF_YEAR, Integer.parseInt(day.substring(day.indexOf("年") + 1, day.indexOf("周"))));
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
        } else if (day.contains("年")) {
            ca.set(Integer.parseInt(day.substring(0, 4)), 0, 1);
        }
        return ca;
    }

    public Calendar noChineseDateArr(String date) throws UDFArgumentLengthException {

        if (date == null) {
            throw new UDFArgumentLengthException("date is null , " + date);
        }
        Integer[] iarr = null;
        if (date.length() == 19) {
            iarr = new Integer[]{Integer.parseInt(date.substring(0, 4)),
                    (Integer.parseInt(date.substring(5, 7)) - 1),
                    Integer.parseInt(date.substring(8, 10)),
                    Integer.parseInt(date.substring(11, 13)),
                    Integer.parseInt(date.substring(14, 16)),
                    Integer.parseInt(date.substring(17, 19))};
        } else if (date.length() == 10) {
            iarr = new Integer[]{Integer.parseInt(date.substring(0, 4)),
                    (Integer.parseInt(date.substring(5, 7)) - 1),
                    Integer.parseInt(date.substring(8, 10)),
                    0, 0, 0};
        } else {
            try {
                iarr = new Integer[]{Integer.parseInt(date.substring(0, 4)),
                        (Integer.parseInt(date.substring(4, 6)) - 1),
                        Integer.parseInt(date.substring(6, 8)),
                        0, 0, 0};
            } catch (Exception e) {
                throw new RuntimeException(date);
            }
        }

        Calendar ca = Calendar.getInstance();
        ca.set(iarr[0],
                iarr[1],
                iarr[2],
                iarr[3],
                iarr[4],
                iarr[5]);
        return ca;
    }

    public static boolean isContainChinese(String str) {
        Pattern p = Pattern.compile("[\u4e00-\u9fa5]");
        Matcher m = p.matcher(str);
        if (m.find()) {
            return true;
        }
        return false;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {

        Calendar ca = Calendar.getInstance();
        logger.info(deferredObjects[0].getClass().getName());
        if (this.timestampObjectInspector01 != null && this.stringObjectInspector02 != null) {
            Timestamp t = timestampObjectInspector01.getPrimitiveJavaObject(deferredObjects[0].get());
            assert ca != null;
            ca.setTime(new Date(t.getTime()));
        } else if (this.longObjectInspector01 != null && this.stringObjectInspector02 != null) {
            long l = longObjectInspector01.get(deferredObjects[0].get());
            assert ca != null;
            ca.setTime(new Date(l));
        } else if (this.stringObjectInspector01 != null && this.stringObjectInspector02 != null) {

            String args0 = stringObjectInspector01.getPrimitiveJavaObject(deferredObjects[0].get());
            if (isContainChinese(args0)) {
                ca = ChineseDateFormat(args0);
            } else {
                ca = noChineseDateArr(args0);
            }
        } else if (this.dateObjectInspector01 != null && this.stringObjectInspector02 != null) {

            Date date = dateObjectInspector01.getPrimitiveJavaObject(deferredObjects[0].get());
            ca.setTime(date);
        } else {
            throw new RuntimeException(String.format("wrong type 【%s\t%s】", deferredObjects[0].getClass().getName(), deferredObjects[1].getClass().getName()));
        }

        String format = this.stringObjectInspector02.getPrimitiveJavaObject(deferredObjects[1].get());

        if (ca == null) {
            throw new UDFArgumentLengthException("date is null , " + ca);
        }

        String result = "";
        switch (format) {
            case "y":
                result = String.format("%d年", ca.get(Calendar.YEAR));
                break;
            case "yq":
                int m = ca.get(Calendar.MONTH);
                if (m < 3) {
                    result = String.format("%d年1季度", ca.get(Calendar.YEAR));
                    break;
                } else if (m < 6) {
                    result = String.format("%d年2季度", ca.get(Calendar.YEAR));
                    break;
                } else if (m < 9) {
                    result = String.format("%d年3季度", ca.get(Calendar.YEAR));
                    break;
                } else {
                    result = String.format("%d年4季度", ca.get(Calendar.YEAR));
                    break;
                }
            case "ym": {
                String mint = (ca.get(Calendar.MONTH) + 1) + "";
                if (mint.length() < 2) {
                    mint = "0" + mint;
                }

                result = String.format("%d年%s月", ca.get(Calendar.YEAR), mint);
                break;
            }
            case "yw":
                result = String.format("%d年%d周", ca.get(Calendar.YEAR), ca.get(Calendar.WEEK_OF_YEAR));
                break;
            case "ymd":
                String mon = Integer.toString(ca.get(Calendar.MONTH) + 1);
                String d = Integer.toString(ca.get(Calendar.DAY_OF_MONTH));
                if (mon.length() < 2) {
                    mon = "0" + mon;
                }
                if (d.length() < 2) {
                    d = "0" + d;
                }
                result = String.format("%d年%s月%s日", ca.get(Calendar.YEAR), mon, d);
                break;
            case "ymdh":
                String mond = Integer.toString(ca.get(Calendar.MONTH) + 1);
                String day = Integer.toString(ca.get(Calendar.DAY_OF_MONTH));
                if (mond.length() < 2) {
                    mond = "0" + mond;
                }
                if (day.length() < 2) {
                    day = "0" + day;
                }
                result = String.format("%d年%s月%s日%d时", ca.get(Calendar.YEAR), mond, day, ca.get(Calendar.HOUR_OF_DAY));
                break;
            default:
                throw new RuntimeException("nonexistent dimen_mode. [y,yq,ym,yw,ymd,ymdh]]");

        }
        return result;
    }

    @Override
    public String getDisplayString(String[] children) {
        return children[0];
    }
}
