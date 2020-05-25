package com.hll.udf.v4;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;

import java.sql.Timestamp;
import java.util.*;

import static com.sun.org.apache.xalan.internal.xsltc.compiler.util.Type.Int;

public class UDFRowColStat extends UDF {

    public Map<String, String> evaluate(Map<String, String> t1, Integer rowcol, List<String> row_func, List<String> col_func,
                                        List<String> length) throws UDFArgumentException {


        return null;

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

public String mkString(List<String > l ,String s){
      return   l.stream().reduce((a,b)->a+s+b).get();

}

public List<String > doRowCount( Map<String, String> m,int dimension_length,int compare_length){
        Map<String ,Double > hangheji = new HashMap<>();
        Map<String ,Double > shuzhixiaoji = new HashMap<>();
        Map<String ,Double > fenliexiaoji = new HashMap<>();

    for (Map.Entry<String, String> en:
       m.entrySet()  ) {
        String[] key = en.getKey().split(",");
        String[] value = en.getValue().split("::");
        List<String> dimensionKey = splitArray(key, 0, dimension_length);
        List<String> compareKey = splitArray(key,dimension_length,dimension_length + compare_length);
        Double v = Arrays.stream(value).map(Double::parseDouble).reduce(Double::sum).get();
        String dKey = mkString(dimensionKey, "△");
        hangheji .put (dKey, (v + hangheji.getOrDefault(dKey, 0.00)));


    }
}
    def doRowCount(m: Map[String, String], dimension_length: Int, compare_length: Int): Seq[String] = {

        var hangheji = Map[String, Double]()
        var shuzhixiaoji = Map[String, Double]()
        var fenliexiaoji = Map[String, Double]()

        m.foreach(en => {
                val key = en._1.split(",")
                val value = en._2.split("::")
                val dimensionKey = splitArray(key, 0, dimension_length)
                val compareKey = splitArray(key, dimension_length, dimension_length + compare_length)

                val v = value.map(_.toDouble).sum

                hangheji += (dimensionKey.mkString("△") -> (v + hangheji.getOrElse(dimensionKey.mkString("△"), 0.00)))
        // 数值小计
        for (i <- 0 until value.length) {
            val shuzhikey = dimensionKey.mkString("△") + "△" + i
            shuzhixiaoji += (shuzhikey -> (value(i).toDouble + shuzhixiaoji.getOrElse(shuzhikey, 0.00)))
        }
        // 分列小计
        val fenliekey = dimensionKey.mkString("△") + "△" + compareKey.mkString("△")
        fenliexiaoji += (fenliekey -> (v + fenliexiaoji.getOrElse(fenliekey, 0.00)))
    })

        var newResultSeq = Seq[String]()
        m.foreach(en => {
                val key = en._1
                var value = en._2

                val krr = key.split(",")
                val vrr = key.split("::")
                val dimensionKey = splitArray(krr, 0, dimension_length)
                val compareKey = splitArray(krr, dimension_length, dimension_length + compare_length)

        for (i <- 0 until vrr.length) {
            val shuzhixiaojiValue = shuzhixiaoji.getOrElse(dimensionKey.mkString("△") + "△" + i, 0.00)
            value = value + "::" + shuzhixiaojiValue
        }
        val fenliexiaojiValue = fenliexiaoji.getOrElse(dimensionKey.mkString("△") + "△" + compareKey.mkString("△"), 0.00)
        value = value + "::" + fenliexiaojiValue
        val hanghejiValue = hangheji.getOrElse(dimensionKey.mkString("△"), 0.00)
        value = value + "::" + hanghejiValue

        if (key.contains("总计")) {
            value = value + "::columnSum"
        } else if (key.contains("小计")) {
            value = value + "::columnSum_subtotal_"
        } else {
            value = value + "::"
        }

        newResultSeq = newResultSeq :+ (key + ":" + value)
    })

        newResultSeq
    }


}
