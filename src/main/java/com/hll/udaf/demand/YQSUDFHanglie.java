package com.hll.udaf.demand;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.StringUtils;

import java.text.MessageFormat;
import java.util.*;

@Description(name = "yqsudf_hanglie",
        value = "_FUNC_(weidu,duibi,shuzhi,limit) - Returns the mean of a set of numbers")
public class YQSUDFHanglie extends AbstractGenericUDAFResolver {

    static final Log logger = LogFactory.getLog(YQSUDFHanglie.class.getName());

    //读入参数类型校验，满足条件时返回聚合函数数据处理对象
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
            throws SemanticException {
        if (parameters.length != 4) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "Exactly one argument is expected.");
        }

        if ((parameters[0].getCategory() != ObjectInspector.Category.LIST
                || parameters[1].getCategory() != ObjectInspector.Category.LIST
                || parameters[2].getCategory() != ObjectInspector.Category.LIST)) {
            throw new UDFArgumentTypeException(0,
                    MessageFormat.format(
                            "Only LIST type arguments are accepted but 1:{0} 2:{1} 3:{2} is passed.", parameters[0].getTypeName(), parameters[1].getTypeName(), parameters[2].getTypeName()));
        }

        /*switch (((PrimitiveTypeInfo) parameters[0]).getPrimitiveCategory()) {
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case STRING:
            case TIMESTAMP:
                return new GenericUDAFAverageEvaluator();
            case BOOLEAN:
            default:
                throw new UDFArgumentTypeException(0,
                        "Only numeric or string type arguments are accepted but "
                                + parameters[0].getTypeName() + " is passed.");
        }*/
        return new GenericUDAFAverageEvaluator();
    }

    /**
     * GenericUDAFAverageEvaluator.
     * 自定义静态内部类：数据处理类，继承GenericUDAFEvaluator抽象类
     */
    public static class GenericUDAFAverageEvaluator extends GenericUDAFEvaluator {

        //1.1.定义全局输入输出数据的类型OI实例，用于解析输入输出数据
        // input For PARTIAL1 and COMPLETE
        StandardListObjectInspector dimension;
        StandardListObjectInspector compare;
        StandardListObjectInspector measure;
        PrimitiveObjectInspector limition;

        // input For PARTIAL2 and FINAL
        // output For PARTIAL1 and PARTIAL2
        StructObjectInspector soi;
        StructField dogField;
        StandardMapObjectInspector dogFieldOI;

        //1.2.定义全局输出数据的类型，用于存储实际数据
        // output For PARTIAL1 and PARTIAL2
        Object[] partialResult;

        // output For FINAL and COMPLETE
        ArrayPrimitiveWritable result;

        @Override
        public ObjectInspector init(Mode mode, ObjectInspector[] parameters)
                throws HiveException {
            assert (parameters.length == 1);
            super.init(mode, parameters);

            // init input
            if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
                dimension = (StandardListObjectInspector) parameters[0];
                compare = (StandardListObjectInspector) parameters[1];
                measure = (StandardListObjectInspector) parameters[2];
                limition = (PrimitiveObjectInspector) parameters[3];
            } else {
                //部分数据作为输入参数时，用到的struct的OI实例，指定输入数据类型，用于解析数据
                soi = (StructObjectInspector) parameters[0];
                dogField = soi.getStructFieldRef("dog");
                //数组中的每个数据，需要其各自的基本类型OI实例解析
                dogFieldOI = (StandardMapObjectInspector) dogField.getFieldObjectInspector();
            }

            // init output
            if (mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2) {
                // The output of a partial aggregation is a struct containing
                // a "long" count and a "double" sum.
                //部分聚合结果是一个数组
                partialResult = new Object[1];
                partialResult[0] = new HashMap<String, String>();
                /*
                 * .构造Struct的OI实例，用于设定聚合结果数组的类型
                 * .需要字段名List和字段类型List作为参数来构造
                 */
                ArrayList<String> fname = new ArrayList<String>();
                fname.add("dog");
                ArrayList<ObjectInspector> foi = new ArrayList<ObjectInspector>();
                //注：此处的两个OI类型 描述的是 partialResult[] 的两个类型，故需一致
                foi.add(ObjectInspectorFactory.getStandardMapObjectInspector(
                        PrimitiveObjectInspectorFactory.javaStringObjectInspector, PrimitiveObjectInspectorFactory.javaStringObjectInspector));
                return ObjectInspectorFactory.getStandardStructObjectInspector(fname, foi);
            } else {
                //FINAL 最终聚合结果为一个数值，并用基本类型OI设定其类型
                result = new ArrayPrimitiveWritable();
                return ObjectInspectorFactory.getStandardListObjectInspector(
                        PrimitiveObjectInspectorFactory.javaStringObjectInspector);
            }
        }

        /*
         * .聚合数据缓存存储结构
         */
        static class BufferAgg implements AggregationBuffer {
            Map<String, String> buffer;
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            BufferAgg result = new BufferAgg();
            reset(result);
            return result;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            BufferAgg myagg = (BufferAgg) agg;
            myagg.buffer = new HashMap<>();
        }

        boolean warned = false;

        /*
         * .遍历原始数据
         */
        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters)
                throws HiveException {
            assert (parameters.length == 4);
            Object pDimension = parameters[0];
            Object pCompare = parameters[1];
            Object pMeasure = parameters[2];
            Object pLimition = parameters[3];

            if (pDimension != null && pCompare != null && pMeasure != null && pLimition != null) {
                logger.info(pDimension + "\001" + pCompare + "\001" + pMeasure + "\001" + pLimition + "\001");
                BufferAgg myagg = (BufferAgg) agg;
                try {
                    //通过基本数据类型OI解析Object p的值
                    List<Object> dimensionObj = (List<Object>) ObjectInspectorUtils.copyToStandardJavaObject(pDimension, dimension);
                    List<Object> compareObj = (List<Object>) ObjectInspectorUtils.copyToStandardJavaObject(pCompare, compare);
                    List<Object> measureObj = (List<Object>) ObjectInspectorUtils.copyToStandardJavaObject(pMeasure, measure);
                    int anInt = PrimitiveObjectInspectorUtils.getInt(pLimition, limition);

                    String sDimension = list2string(dimensionObj, "\001");
                    String sCompare = list2string(compareObj, "\001");
                    String sMeasure = list2string(measureObj, "\001");

                    String orDefault = myagg.buffer.getOrDefault(sDimension, "");
                    if (!orDefault.equals("")) {
                        orDefault = orDefault + ":" + sCompare + "=" + sMeasure;
                    } else {
                        orDefault = sCompare + "=" + sMeasure;
                    }
                    myagg.buffer.put(sDimension, orDefault);
                    myagg.buffer.put("anInt", String.format("%d", anInt));
                } catch (NumberFormatException e) {
                    if (!warned) {
                        warned = true;
                        logger.warn(getClass().getSimpleName() + " "
                                + StringUtils.stringifyException(e));
                        logger.warn(getClass().getSimpleName()
                                + " ignoring similar exceptions.");
                    }
                }
            }
        }

        /*
         * .得出部分聚合结果
         */
        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            BufferAgg myagg = (BufferAgg) agg;
//            partialResult[0] = myagg.buffer;
            HashMap<String, String> dog = (HashMap<String, String>) partialResult[0];
            dog.putAll(myagg.buffer);

            return partialResult;
        }

        /*
         * .合并部分聚合结果
         * .注：Object[] 是 Object 的子类，此处 partial 为 Object[]数组
         */
        @Override
        public void merge(AggregationBuffer agg, Object partial)
                throws HiveException {

            if (partial != null) {
                BufferAgg myagg = (BufferAgg) agg;
                //通过StandardStructObjectInspector实例，分解出 partial 数组元素值
                Object partialDog = soi.getStructFieldData(partial, dogField);
                HashMap<String, String> dog = (HashMap<String, String>) dogFieldOI.getMap(partialDog);
                String anInt = dog.getOrDefault("anInt", "1000");
                dog.remove("anInt");

                //通过基本数据类型的OI实例解析Object的值
                Set<String> newset = new HashSet<>();
                Set<String> keyset = dog.keySet();
                Set<String> bufferKeyset = myagg.buffer.keySet();
                newset.addAll(keyset);
                newset.addAll(bufferKeyset);

                for (String key : newset) {

                    String value = dog.getOrDefault(key, "");
                    String bufferValue = myagg.buffer.getOrDefault(key, "");

                    if (bufferValue.equals("")) {
                        bufferValue = value;
                    } else {
                        String[] v1rr = value.split(":");
                        String[] bv1rr = bufferValue.split(":");
//                        List<Object> strings = Arrays.asList(bv1rr);
                        List<Object> strings = new ArrayList<>(bv1rr.length);
                        strings.addAll(Arrays.asList(bv1rr));
                        for (String s : v1rr) {
                            if (!strings.contains(s)) {
                                strings.add(s);
                            }
                        }
                        bufferValue = list2string(strings, ":");
                    }
                    myagg.buffer.put(key, bufferValue);
                }
                myagg.buffer.put("anInt", anInt);
            }
        }

        /*
         * .得出最终聚合结果
         */
        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            List<String> result = new LinkedList<>();

            BufferAgg myagg = (BufferAgg) agg;

            int limit = Integer.parseInt(myagg.buffer.getOrDefault("anInt", "1000"));
            int count = 0;
            for (Map.Entry<String, String> en :
                    myagg.buffer.entrySet()) {
                if (count <= limit) {
                    if (!en.getKey().equals("anInt")) {
                        result.add(en.getKey() + "::" + en.getValue());
                    }
                    count++;
                }
            }
            return result;
        }

        public String list2string(List<Object> l, String d) {
            if (l != null && l.size() > 0) {
                return l.stream().map(a -> a.toString()).reduce((a, b) -> a + d + b).get();
            } else {
                return "";
            }

        }
    }
}