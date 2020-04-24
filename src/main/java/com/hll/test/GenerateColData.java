package com.hll.test;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GenerateColData {

    public static void main(String[] args) throws Exception {

        readFile("col.txt");

    }

    public static void readFile(String pathname) throws Exception {
        try (FileReader reader = new FileReader(pathname);
             BufferedReader br = new BufferedReader(reader) // 建立一个对象，它把文件内容转成计算机能读懂的语言
        ) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] larr = line.split(",");
                List<String> args01 = new ArrayList<>();
                List<String> args02 = new ArrayList<>();
                List<String> args03 = new ArrayList<>();
                args01.add(larr[0]);
                args01.add(larr[0]);
                args01.add(larr[0]);
                args02.add(larr[6]);
                args02.add(larr[7]);
                args02.add(larr[8]);
                args03.add("compare-5-0-");
                args03.add("compare-5-1-");
                args03.add("compare-5-2-");
            }


        } catch (IOException e) {
            e.printStackTrace();
        }
    }



}
