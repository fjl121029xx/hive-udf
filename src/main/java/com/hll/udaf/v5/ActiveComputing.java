package com.hll.udaf.v5;

import com.hll.udaf.v4.NewAddStat;
import com.hll.udaf.v4.RetentionComputing;
import org.apache.hadoop.hive.ql.exec.Description;
import com.hll.udaf.v3.GenericUDAFAverage;
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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.*;

import java.util.*;

@Description(name = "udfretentioncomputing",
        value = "_func_(dimensions, measures, measurefunc) -  " +
                " list<string> , list<string> , list<string>p")
public class ActiveComputing extends AbstractGenericUDAFResolver {

    static final Log LOG = LogFactory.getLog(GenericUDAFAverage.class.getName());

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
            throws SemanticException {
        if (parameters.length != 3) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "Exactly one argument is expected.");
        }

        if (parameters[0].getCategory() != ObjectInspector.Category.LIST) {
            throw new UDFArgumentTypeException(0,
                    "Only list arguments are accepted but "
                            + parameters[0].getTypeName() + " is passed.");
        }
        if (parameters[1].getCategory() != ObjectInspector.Category.LIST) {
            throw new UDFArgumentTypeException(1,
                    "Only list arguments are accepted but "
                            + parameters[1].getTypeName() + " is passed.");
        }
        if (parameters[2].getCategory() != ObjectInspector.Category.LIST) {
            throw new UDFArgumentTypeException(2,
                    "Only list arguments are accepted but "
                            + parameters[2].getTypeName() + " is passed.");
        }
        return new GenericUDAFAverageEvaluator();
    }

    public static class GenericUDAFAverageEvaluator extends GenericUDAFEvaluator {

        //1.1.定义全局输入输出数据的类型OI实例，用于解析输入输出数据
        // input For PARTIAL1 and COMPLETE
        StandardListObjectInspector dimensionsInspector;
        StandardListObjectInspector measureInspector;
        StandardListObjectInspector highMathInspector;

        // input For PARTIAL2 and FINAL
        // output For PARTIAL1 and PARTIAL2
        StructObjectInspector soi;
        StructField catField;
        StructField dogField;
        StructField fishField;
        StructField pigField;

        StandardListObjectInspector catFieldOI;
        StringObjectInspector dogFieldOI;
        StandardMapObjectInspector fishFieldOI;
        StandardMapObjectInspector pigFieldOI;

        //1.2.定义全局输出数据的类型，用于存储实际数据
        // output For PARTIAL1 and PARTIAL2
        Object[] partialResult;

        // output For FINAL and COMPLETE
        MapWritable result;

        @Override
        public ObjectInspector init(Mode mode, ObjectInspector[] parameters)
                throws HiveException {
            assert (parameters.length == 3);
            super.init(mode, parameters);

            if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
                dimensionsInspector = (StandardListObjectInspector) parameters[0];
                measureInspector = (StandardListObjectInspector) parameters[1];
                highMathInspector = (StandardListObjectInspector) parameters[1];
            } else {
                //部分数据作为输入参数时，用到的struct的OI实例，指定输入数据类型，用于解析数据
                soi = (StructObjectInspector) parameters[0];
                catField = soi.getStructFieldRef("cat");
                dogField = soi.getStructFieldRef("dog");
                fishField = soi.getStructFieldRef("fish");
                pigField = soi.getStructFieldRef("pig");
                //数组中的每个数据，需要其各自的基本类型OI实例解析
                catFieldOI = (StandardListObjectInspector) catField.getFieldObjectInspector();
                dogFieldOI = (StringObjectInspector) dogField.getFieldObjectInspector();
                fishFieldOI = (StandardMapObjectInspector) fishField.getFieldObjectInspector();
                pigFieldOI = (StandardMapObjectInspector) pigField.getFieldObjectInspector();
            }

            // init output
            if (mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2) {

                partialResult = new Object[4];
                partialResult[0] = new LinkedList<String>();
                partialResult[1] = "";
                partialResult[2] = new HashMap<String, Integer>();
                partialResult[3] = new HashMap<String, Integer>();

                ArrayList<String> fname = new ArrayList<String>();
                fname.add("cat");
                fname.add("dog");
                fname.add("fish");
                fname.add("pig");

                ArrayList<ObjectInspector> foi = new ArrayList<>();
                foi.add(ObjectInspectorFactory.getStandardListObjectInspector(
                        PrimitiveObjectInspectorFactory.javaStringObjectInspector));
                foi.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
                // fish
                foi.add(ObjectInspectorFactory.getStandardMapObjectInspector(
                        PrimitiveObjectInspectorFactory.javaStringObjectInspector, PrimitiveObjectInspectorFactory.javaIntObjectInspector));
                // pig
                foi.add(ObjectInspectorFactory.getStandardMapObjectInspector(
                        PrimitiveObjectInspectorFactory.javaStringObjectInspector, PrimitiveObjectInspectorFactory.javaIntObjectInspector));

                return ObjectInspectorFactory.getStandardStructObjectInspector(fname, foi);
            } else {
                result = new MapWritable();
                return ObjectInspectorFactory.getStandardMapObjectInspector(
                        PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                        PrimitiveObjectInspectorFactory.javaStringObjectInspector);
            }
        }

        static class AverageAgg implements AggregationBuffer {
            List<String> cat;
            String dog;
            HashMap<String, Integer> fish;
            HashMap<String, Integer> pig;
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            GenericUDAFAverageEvaluator.AverageAgg result = new GenericUDAFAverageEvaluator.AverageAgg();
            reset(result);
            return result;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            GenericUDAFAverageEvaluator.AverageAgg myagg = (GenericUDAFAverageEvaluator.AverageAgg) agg;
            myagg.cat = new LinkedList<>();
            myagg.dog = "";
            myagg.fish = new HashMap<>();
            myagg.pig = new HashMap<>();
        }


        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters)
                throws HiveException {
            assert (parameters.length == 3);

            GenericUDAFAverageEvaluator.AverageAgg myagg = (GenericUDAFAverageEvaluator.AverageAgg) agg;
            Object p1 = parameters[0];
            Object p2 = parameters[1];
            Object p3 = parameters[2];
            if (p1 != null && p2 != null && p3 != null) {

                List<String> dimension = (List<String>) ObjectInspectorUtils.copyToStandardJavaObject(p1, dimensionsInspector);
                List<String> measure = (List<String>) ObjectInspectorUtils.copyToStandardJavaObject(p2, measureInspector);
                List<String> match_func = (List<String>) ObjectInspectorUtils.copyToStandardJavaObject(p3, highMathInspector);

                for (int i = 0; i < Math.min(dimension.size(), measure.size()); i++) {
                    String a = dimension.get(i);
                    String b = measure.get(i) + "";
                    myagg.cat.add(a + ":" + b);
                }
                myagg.dog = match_func.get(0);

                myagg.fish.put(dimension.get(0), 1);
                myagg.pig.put(measure.get(0), 1);
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            GenericUDAFAverageEvaluator.AverageAgg myagg = (GenericUDAFAverageEvaluator.AverageAgg) agg;
            List<String> cat = myagg.cat;
            LinkedList<String> list = (LinkedList<String>) partialResult[0];
            for (int i = 0; i < cat.size(); i++) {
                list.add(cat.get(i));
            }
            partialResult[1] = myagg.dog;

            HashMap<String, Integer> fish = (HashMap<String, Integer>) partialResult[2];
            fish.putAll(myagg.fish);

            HashMap<String, Integer> pig = (HashMap<String, Integer>) partialResult[3];
            pig.putAll(myagg.pig);
            return partialResult;
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial)
                throws HiveException {
            if (partial != null) {
                GenericUDAFAverageEvaluator.AverageAgg myagg = (GenericUDAFAverageEvaluator.AverageAgg) agg;
                //通过StandardStructObjectInspector实例，分解出 partial 数组元素值
                Object partialCat = soi.getStructFieldData(partial, catField);
                Object partialDog = soi.getStructFieldData(partial, dogField);

                Object partialFish = soi.getStructFieldData(partial, fishField);
                Object partialPig = soi.getStructFieldData(partial, pigField);


                List<String> cat = (List<String>) catFieldOI.getList(partialCat);
                String dog = dogFieldOI.getPrimitiveJavaObject(partialDog);
                myagg.cat.addAll(cat);
                myagg.dog = (dog != null && !dog.equals("")) ? dog : myagg.dog;


                HashMap<String, Integer> fish = (HashMap<String, Integer>) fishFieldOI.getMap(partialFish);
                myagg.fish.putAll(fish);

                HashMap<String, Integer> pig = (HashMap<String, Integer>) pigFieldOI.getMap(partialPig);
                myagg.pig.putAll(pig);

            }
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            GenericUDAFAverageEvaluator.AverageAgg myagg = (GenericUDAFAverageEvaluator.AverageAgg) agg;

            List<String> cat = myagg.cat;
            cat.sort(String::compareTo);

            String dog = myagg.dog;

            ActiveStat activestat = new ActiveStat(cat, dog, myagg.fish.keySet(), myagg.pig.keySet());
            try {
                return activestat.compute();
            } catch (InterruptedException e) {
                e.printStackTrace();
                return new HashMap<>();
            }
        }
    }

}
