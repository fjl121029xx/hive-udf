package com.hll.test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ProxyUDF implements Udf {

    Udf udf;
    public ProxyUDF(Udf udf) {
        this.udf = udf;
    }

    @Override
    public boolean iterate(Map<String, Map<String, String>> PartialResult, List<String> dimensions, List<String> measure, List<String> mathFunction) {

        try (FileReader reader = new FileReader("col.txt");
             BufferedReader br = new BufferedReader(reader) // 建立一个对象，它把文件内容转成计算机能读懂的语言
        ) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] larr = line.split(",");
                List<String> args01 = new ArrayList<>();
                List<String> args02 = new ArrayList<>();
                List<String> args03 = new ArrayList<>();

                args01.add(larr[6]);
                args01.add(larr[7]);
                args01.add(larr[8]);

                args02.add(larr[0]);
                args02.add(larr[0]);
                args02.add(larr[0]);

                args03.add("compare-5-0-");
                args03.add("compare-5-1-");
                args03.add("compare-5-2-");

                udf.iterate(PartialResult, args01, args02, args03);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }

    @Override
    public Map<String, Map<String, String>> terminatePartial() {
        return udf.terminatePartial();
    }

    @Override
    public boolean merge(Map<String, Map<String, String>> PartialResult, Map<String, Map<String, String>> mapOutput) {
        return udf.merge(PartialResult, mapOutput);
    }

    @Override
    public Map<String, String> terminate() {
        return udf.terminate();
    }
}
