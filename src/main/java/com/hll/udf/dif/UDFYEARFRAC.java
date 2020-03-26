package com.hll.udf.dif;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.Date;

public class UDFYEARFRAC extends GenericUDF {

    private DateObjectInspector dateObjectInspector01;
    private DateObjectInspector dateObjectInspector02;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length != 2) {
            throw new UDFArgumentLengthException("arrayContainsExample only takes 2 arguments: date,  date");
        }
        // 1. 检查是否接收到正确的参数类型
        ObjectInspector a = objectInspectors[0];
        ObjectInspector b = objectInspectors[1];
        if (!(a instanceof DateObjectInspector) || !(b instanceof DateObjectInspector)) {
            throw new UDFArgumentException("first argument must be a String, second argument must be a String");
        }

        this.dateObjectInspector01 = (DateObjectInspector) a;
        this.dateObjectInspector02 = (DateObjectInspector) b;

        return PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
    }


    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {

        Date start = this.dateObjectInspector01.getPrimitiveJavaObject(deferredObjects[0].get());
        Date end = this.dateObjectInspector02.getPrimitiveJavaObject(deferredObjects[1].get());

        if (start == null || end == null) {
            throw new UDFArgumentLengthException(String.format("args has null :=} start is %s end is %s", start, end));
        }
        int year = start.getYear();
        if (year != end.getYear()) {
            throw new UDFArgumentLengthException("start of year year must be same to end of year");

        }
        BigDecimal days;
        if (year % 4 == 0 && year % 100 != 0 || year % 400 == 0) {//闰年的判断规则
            days = new BigDecimal(366);
        } else {
            days = new BigDecimal(365);
        }
        LocalDate startDate = LocalDate.of(year, start.getMonth(), start.getDay());
        LocalDate endDate = LocalDate.of(end.getYear(), end.getMonth(), end.getDay());
        BigDecimal between = new BigDecimal(ChronoUnit.WEEKS.between(startDate, endDate));

        return between.divide(days, RoundingMode.HALF_UP).doubleValue();
    }

    @Override
    public String getDisplayString(String[] children) {
        return children[0];
    }

    public static void main(String[] args) throws InterruptedException {
//        calculateTimeDifferenceByChronoUnit();
//        calculateTimeDifferenceByDuration();
        calculateTimeDifferenceByDuration();

        Clock utcClock = Clock.systemUTC();

        Instant inst1 = Instant.now(utcClock);
        System.out.println(inst1);
        Thread.sleep(10000);
        Instant inst2 = Instant.ofEpochSecond(new java.util.Date().getTime() / 1000);
        System.out.println(inst2);
    }

    public static void calculateTimeDifferenceByDuration() {
        Instant inst1 = Instant.now();  //当前的时间
        System.out.println("Inst1：" + inst1);
        Instant inst2 = inst1.plus(Duration.ofSeconds(10));     //当前时间+10秒后的时间
        System.out.println("Inst2：" + inst2);
        Instant inst3 = inst1.plus(Duration.ofDays(125));       //当前时间+125天后的时间
        System.out.println("inst3：" + inst3);

        System.out.println("以毫秒计的时间差：" + Duration.between(inst1, inst2).toMillis());

        System.out.println("以秒计的时间差：" + Duration.between(inst1, inst3).getSeconds());
    }

    public static void calculateTimeDifferenceByChronoUnit() {
        LocalDate startDate = LocalDate.of(2003, Month.MAY, 1);
        System.out.println("开始时间：" + startDate);

        LocalDate endDate = LocalDate.of(2015, Month.JANUARY, 26);
        System.out.println("结束时间：" + endDate);

        long daysDiff = ChronoUnit.DAYS.between(startDate, endDate);
        System.out.println("两个时间之间的天数差为：" + daysDiff);

//        long daysDiff2 = ChronoUnit.HOURS.between(startDate, endDate);
//        System.out.println("两个时间之间的小时差为：" + daysDiff2);

        long daysDiff3 = ChronoUnit.WEEKS.between(startDate, endDate);
        System.out.println("两个时间之间的星期数差为：" + daysDiff3);

        long daysDiff4 = ChronoUnit.YEARS.between(startDate, endDate);
        System.out.println("两个时间之间的年数差为：" + daysDiff4);
    }
}
