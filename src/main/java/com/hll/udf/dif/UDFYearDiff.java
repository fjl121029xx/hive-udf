package com.hll.udf.dif;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.Date;

public class UDFYearDiff extends GenericUDF {
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

        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }


    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {

        Date start = this.dateObjectInspector01.getPrimitiveJavaObject(deferredObjects[0].get());
        Date end = this.dateObjectInspector02.getPrimitiveJavaObject(deferredObjects[1].get());

        if (start == null || end == null) {
            throw new UDFArgumentLengthException(String.format("args has null :=} start is %s end is %s", start, end));
        }

        LocalDate startDate = LocalDate.of(start.getYear(), start.getMonth(), start.getDay());
        LocalDate endDate = LocalDate.of(end.getYear(), end.getMonth(), end.getDay());

        return ChronoUnit.YEARS.between(startDate, endDate);
    }

    @Override
    public String getDisplayString(String[] children) {
        return children[0];
    }

}
