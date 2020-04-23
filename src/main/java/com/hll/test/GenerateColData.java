package com.hll.test;

import java.io.*;

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
                String[] larr = line.split("\t");
                for (String s :larr) {
                    System.out.print(s);
                    System.out.print("--");
                }
                System.out.println();

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
