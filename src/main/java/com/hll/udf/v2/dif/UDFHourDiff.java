package com.hll.udf.v2.dif;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;

import static java.lang.Integer.parseInt;
import static java.math.BigDecimal.ROUND_HALF_DOWN;

@Description(name = "udfhourdiff",
        value = "_FUNC_(start, end) - " +
                "date:Timestamp , day:Timestamp")
public class UDFHourDiff extends GenericUDF {

    private TimestampObjectInspector dateObjectInspector01;
    private TimestampObjectInspector dateObjectInspector02;

    private JavaLongObjectInspector longObjectInspector01;
    private JavaLongObjectInspector longObjectInspector02;

    private JavaStringObjectInspector stringObjectInspector01;
    private JavaStringObjectInspector stringObjectInspector02;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length != 2) {
            throw new UDFArgumentLengthException("arrayContainsExample only takes 2 arguments: date,  date");
        }
        // 1. 检查是否接收到正确的参数类型
        ObjectInspector a = objectInspectors[0];
        ObjectInspector b = objectInspectors[1];


        if (a instanceof TimestampObjectInspector && b instanceof TimestampObjectInspector) {
            this.dateObjectInspector01 = (TimestampObjectInspector) a;
            this.dateObjectInspector02 = (TimestampObjectInspector) b;
        } else if (a instanceof JavaLongObjectInspector && b instanceof JavaLongObjectInspector) {

            this.longObjectInspector01 = (JavaLongObjectInspector) a;
            this.longObjectInspector02 = (JavaLongObjectInspector) b;
        } else if (a instanceof JavaStringObjectInspector && b instanceof JavaStringObjectInspector) {

            this.stringObjectInspector01 = (JavaStringObjectInspector) a;
            this.stringObjectInspector02 = (JavaStringObjectInspector) b;
        } else {
            throw new UDFArgumentException(String.format("first argument must be a Timestamp, second argument must be a Timestamp %s %s", a.getClass(), b.getClass()));
        }
        return PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
    }

    public BigDecimal doTimeStamp(DeferredObject[] deferredObjects, TimestampObjectInspector dateObjectInspector01, TimestampObjectInspector dateObjectInspector02) throws HiveException {


        Timestamp start = dateObjectInspector01.getPrimitiveJavaObject(deferredObjects[0].get());
        Timestamp end = dateObjectInspector02.getPrimitiveJavaObject(deferredObjects[1].get());
        if (start == null || end == null) {
            throw new UDFArgumentLengthException(String.format("args has null :=} start is %s end is %s", start, end));
        }
        Calendar startCa = Calendar.getInstance();
        startCa.setTime(new Date(start.getTime()));
        Calendar endCa = Calendar.getInstance();
        endCa.setTime(new Date(end.getTime()));
        BigDecimal startSecond = new BigDecimal(startCa.getTime().getTime());
        BigDecimal endSecond = new BigDecimal(endCa.getTime().getTime());
        BigDecimal div = new BigDecimal(60 * 60 * 1000);
        try {
            BigDecimal result = endSecond.subtract(startSecond).divide(div).setScale(2, BigDecimal.ROUND_UP);
            return result;
        } catch (Exception e) {
            throw new RuntimeException(String.format("math start %s end %s div %s", startSecond.toString(), endSecond, div));
        }

    }

    public BigDecimal doLong(DeferredObject[] deferredObjects, JavaLongObjectInspector longObjectInspector01, JavaLongObjectInspector longObjectInspector02) throws HiveException {
        long start = longObjectInspector01.get(deferredObjects[0].get());
        long end = longObjectInspector02.get(deferredObjects[1].get());


        if (start == 0 || end == 0) {
            throw new UDFArgumentLengthException(String.format("args has null :=} start is %s end is %s", start, end));
        }
        Calendar startCa = Calendar.getInstance();
        startCa.setTime(new Date(start));
        Calendar endCa = Calendar.getInstance();
        endCa.setTime(new Date(end));
        BigDecimal startSecond = new BigDecimal(startCa.getTime().getTime());
        BigDecimal endSecond = new BigDecimal(endCa.getTime().getTime());
        BigDecimal div = new BigDecimal(60 * 60 * 1000);
        try {
            BigDecimal result = endSecond.subtract(startSecond).divide(div, 2, ROUND_HALF_DOWN).setScale(2, BigDecimal.ROUND_UP);
            return result;
        } catch (Exception e) {
            throw new RuntimeException(String.format("math start %s end %s div %s", startSecond.toString(), endSecond, div));
        }
    }

    public Calendar daDateStr(String reportdate) {
        Calendar ca = Calendar.getInstance();

        if (reportdate.length() == 19) {
            ca.set(parseInt(reportdate.substring(0, 4)),
                    parseInt(reportdate.substring(5, 7)) - 1,
                    parseInt(reportdate.substring(8, 10)),
                    parseInt(reportdate.substring(11, 13)),
                    parseInt(reportdate.substring(14, 16)),
                    parseInt(reportdate.substring(17, 19))
            );
        } else if (reportdate.length() == 10) {
            ca.set(parseInt(reportdate.substring(0, 4)),
                    parseInt(reportdate.substring(5, 7)) - 1,
                    parseInt(reportdate.substring(8, 10))
            );
        } else {
            ca.set(parseInt(reportdate.substring(0, 4)),
                    parseInt(reportdate.substring(4, 6)) - 1,
                    parseInt(reportdate.substring(6, 8))
            );
        }
        return ca;
    }

    public BigDecimal doString(DeferredObject[] deferredObjects, JavaStringObjectInspector stringObjectInspector01, JavaStringObjectInspector stringObjectInspector02) throws HiveException {

        String start = stringObjectInspector01.getPrimitiveJavaObject(deferredObjects[0].get());
        String end = stringObjectInspector02.getPrimitiveJavaObject(deferredObjects[1].get());

        if (start == null || end == null) {
            throw new UDFArgumentLengthException(String.format("args has null :=} start is %s end is %s", start, end));
        }
        Calendar startCa = daDateStr(start);
        Calendar endCa = daDateStr(end);
        BigDecimal startSecond = new BigDecimal(startCa.getTime().getTime());
        BigDecimal endSecond = new BigDecimal(endCa.getTime().getTime());
        BigDecimal div = new BigDecimal(60 * 60 * 1000);
        try {
            BigDecimal result = endSecond.subtract(startSecond).divide(div, 2, ROUND_HALF_DOWN).setScale(2, BigDecimal.ROUND_UP);
            return result;
        } catch (Exception e) {
            throw new RuntimeException(String.format("math start %s end %s div %s", startSecond.toString(), endSecond, div));
        }
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {

        BigDecimal result;

        if (this.dateObjectInspector01 != null && this.dateObjectInspector02 != null) {
            result = doTimeStamp(deferredObjects, this.dateObjectInspector01, this.dateObjectInspector02);
        } else if (this.longObjectInspector01 != null && this.longObjectInspector02 != null) {
            result = doLong(deferredObjects, this.longObjectInspector01, this.longObjectInspector01);
        } else if (this.stringObjectInspector01 != null && this.stringObjectInspector02 != null) {
            result = doString(deferredObjects, this.stringObjectInspector01, this.stringObjectInspector01);
        } else {
            throw new RuntimeException(String.format("wrong type %s\t%s", deferredObjects[0].getClass().getName(), deferredObjects[1].getClass().getName()));
        }
        return result.doubleValue();

    }

    @Override
    public String getDisplayString(String[] children) {
        return children[0];
    }


}
