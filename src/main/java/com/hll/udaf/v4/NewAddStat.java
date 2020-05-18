package com.hll.udaf.v4;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.*;

public class NewAddStat {
    private List<String> add_up;
    private String dog;
    private boolean isValue;
    private byte[] current_bitMap;
    private byte[] current_bitMap2;
    private int current_index = 0;

    //
    private Set<String> keyList;
    private Set<String> idList;


    public NewAddStat(List<String> add_up, String dog, Set<String> keyList, Set<String> idList) {
        this.add_up = add_up;
        String[] split = dog.split("-");
        this.dog = split[0];
        this.isValue = split[1].equals("value");
        //
        this.keyList = keyList;
        this.idList = idList;
    }

    public Map<String, String> compute() {
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");

        System.out.println("start compute " + sdf.format(new Date()));

        byte[][] bits = new byte[keyList.size() + 1][getIndex(idList.size()) + 1];

//        long a1 = System.currentTimeMillis();
//        Map<String, Integer> idMap = new HashMap<>();
//        for (String en : idList) {
//            String[] err = en.split(":");
//            idMap.put(err[0], Integer.parseInt(err[1]));
//        }
//        long a2 = System.currentTimeMillis();
//        System.out.println("idMap 耗时 " + (a2 - a1));
//        System.out.println("id 标记 " + idMap.size());

        long a3 = System.currentTimeMillis();
        Map<String, Integer> keyMap = new HashMap<>();
        List<String> s = new ArrayList<>(keyList);
        s.sort(String::compareTo);
        int keyIndex = 0;
        for (String k : s) {
            if (keyMap.get(k) == null) {
                keyMap.put(k, keyIndex);
                keyIndex++;
            }
        }
        long a4 = System.currentTimeMillis();
        System.out.println("keyMap 耗时 " + (a4 - a3));
        System.out.println("key 标记 " + keyMap.size());

        long a = System.currentTimeMillis();
        long abc = 0;

        for (String en : add_up) {
            long l1 = System.currentTimeMillis();
            String key = en.split(":")[0];
            int index = Integer.parseInt(en.split(":")[1]);
            add(bits, keyMap.get(key), index);

            long l2 = System.currentTimeMillis();
            abc += (l2 - l1);

        }
        System.out.println("abc 耗时 " + abc);


        long b = System.currentTimeMillis();
        System.out.println("标记耗时 " + (b - a));
        // 1->日期
        Map<Integer, String> dayNumMap = new HashMap<>();
        for (String key : keyMap.keySet()) {
            dayNumMap.put(keyMap.get(key), key);
        }
        long e = System.currentTimeMillis();
        System.out.println("反转耗时 " + (e - b));


        Map<String, String> map = doByWhich(dog, bits, dayNumMap, keyMap);
        long end = System.currentTimeMillis();
        System.out.printf("compute time %d%n", (end - b));
        return map;
    }

    private Map<String, String> zeroSave(byte[][] bits, Map<Integer, String> dayNumMap, Map<String, Integer> keyMap) {

        Map<String, String> returnMap = new HashMap<>();
        byte[] first = formatByte(bits[0]);
        int count = 0;
        for (byte b : first) {
            if (b == 1) {
                count++;
            }
        }
        returnMap.put(dayNumMap.get(0), Integer.toString(count));

        for (int i = 0; i < keyMap.size(); i++) {
            int cal = calculate(i, i + 1, bits, dayNumMap);
            String s = dayNumMap.get(i + 1);
            if (s != null) {
                returnMap.put(s, Integer.toString(cal));
            }
        }
        return returnMap;
    }

    public void showByte2Str(byte b) {
        String tString = Integer.toBinaryString((b & 0xFF) + 0x100).substring(1);
        System.out.println(tString);
    }

    public String byte2Str(byte b) {
        String tString = Integer.toBinaryString((b & 0xFF) + 0x100).substring(1);
        return tString;
    }

    public byte bit2byte(String bString, String bString2) {
        byte result = 0;
        for (int i = bString.length() - 1, j = 0; i >= 0; i--, j++) {
            result += (Byte.parseByte(bString.charAt(i) + "") * Math.pow(2, j));
        }
        return result;
    }

    private byte[] getDayNewly(byte[][] bits, int dayIndex) {

        if (dayIndex == 0) {
            this.current_bitMap = formatByte(bits[0]);
            this.current_bitMap2 = bits[0];
            return this.current_bitMap;
        }

        this.current_index += 1;

        byte[] new_bitMap2 = new byte[bits[0].length];

        byte[] g = bits[0];
        byte[] h = bits[this.current_index];
        for (int i = 0; i < g.length; i++) {
            byte b2 = (byte) (g[i] | h[i]);
            new_bitMap2[i] = b2;
        }

        this.current_bitMap = formatByte(new_bitMap2);
        byte[] index_bitMap = bits[dayIndex];

        byte[] result_bitmap = formatByte(new byte[bits[0].length]);

        for (int i = 0; i < new_bitMap2.length; i++) {
            String s = byte2Str(new_bitMap2[i]);
            String s1 = byte2Str(index_bitMap[i]);
            byte[] bytes = s.getBytes();
            byte[] bytes1 = s1.getBytes();

            for (int j = 0; j < bytes.length; j++) {
                if (bytes[j] == 0 && bytes1[j] == 0) {
                    result_bitmap[i * 8 + j] = 1;
                }
            }
        }
        return result_bitmap;
    }


    private Map<String, String> otherSave(String dog, byte[][] bits, Map<Integer, String> dayNumMap, Map<String, Integer> keyMap) {

        Map<String, String> returnMap = new HashMap<>();
        System.out.println("otherSave start");
        int dogInt = Integer.parseInt(dog);

        long exeTime = 0L;

        for (int i = 0; i < keyMap.size(); i++) {

            long l = System.currentTimeMillis();
            byte[] dayNewly = getDayNewly(bits, i);
            int sum_bit = sumBit(dayNewly);
            int afterDays = i + dogInt;
            byte[] bit;
            if (afterDays > bits.length - 1) {
                bit = formatByte(new byte[bits[0].length]);
            } else {
                bit = formatByte(bits[afterDays]);
            }

            int newAdd = 0;
            for (int j = 0; j < bit.length; j++) {
                byte e = dayNewly[j];
                byte s = bit[j];
                if (e == 1 && s == 1) {
                    newAdd += 1;
                }
            }

            long l1 = System.currentTimeMillis();
            exeTime += (l1 - l);
            System.out.println("-----------> " + (l1 - l));
            String s = dayNumMap.get(i);
            if (s != null) {
                if (isValue) {
                    returnMap.put(s, Integer.toString(newAdd));
                } else {
                    BigDecimal a = new BigDecimal(newAdd);
                    BigDecimal b = new BigDecimal(sum_bit);
                    if (sum_bit != 0) {
                        returnMap.put(s, a.divide(b, 9, BigDecimal.ROUND_HALF_UP).toString());
                    } else {
                        returnMap.put(s, Double.toString(0.00));
                    }

                }
            }
        }

        System.out.println("exeTime ================" + exeTime);
        return returnMap;
    }

    // 选择
    public Map<String, String> doByWhich(String dog, byte[][] bits, Map<Integer, String> re_mmap, Map<String, Integer> keyMap) {
        switch (dog) {
            case "new": {
                return zeroSave(bits, re_mmap, keyMap);
            }
            default:
                return otherSave(dog, bits, re_mmap, keyMap);
        }

    }

    public int sumBit(byte[] dayNewly) {
        int count = 0;
        for (int i = 0; i < dayNewly.length; i++) {
            if (dayNewly[i] == 1) {
                count++;
            }
        }
        return count;
    }

    public byte[] intToByte(int val) {
        byte[] b = new byte[4];
        b[0] = (byte) (val & 0xff);
        b[1] = (byte) ((val >> 8) & 0xff);
        b[2] = (byte) ((val >> 16) & 0xff);
        b[3] = (byte) ((val >> 24) & 0xff);
        return b;
    }

    public int calculate(int from, int to, byte[][] bits, Map<Integer, String> re_mmap) {

        byte[] bitnext_tmp = formatByte(new byte[bits[from].length]);
        for (int j = 0; j <= from; j++) {

            byte[] bit = formatByte(bits[j]);
            for (int i = 0; i < bitnext_tmp.length; i++) {
                byte e = bitnext_tmp[i];
                byte s = bit[i];
                int tmp;
                if ((tmp = e | s) == 1) {
                    bitnext_tmp[i] = 1;
                } else {
                    bitnext_tmp[i] = 0;
                }
            }
        }

        byte[] a = bitnext_tmp;
        byte[] bit1 = bits[to];
        byte[] b = formatByte(bit1);

        int newAdd = 0;
        for (int i = 0; i < a.length; i++) {
            byte s = a[i];
            byte e = b[i];
            if (s == 0 && e == 1) {
                newAdd += 1;
            }
        }
        return newAdd;
    }

    public static byte[] formatByte(byte[] b) {
        byte[] newb = new byte[b.length];
        for (int n = 0; n < newb.length; n++) {
            newb[n] = b[n];
        }
        byte[] array = new byte[8 * newb.length];
        for (int j = 0; j < b.length; j++) {
            for (int i = (7 + j * 8); i >= (0 + j * 8); i--) {
                array[i] = (byte) (newb[j] & 1);
                newb[j] = (byte) (newb[j] >> 1);
            }
        }
        return array;
    }

    public static void printByte(byte[] array) {
        for (byte b1 : array) {
            System.out.print(b1);
            System.out.print(" ");
        }
        System.out.println();
    }

    public static void showByte(byte b) {
        byte[] array = new byte[8];
        for (int i = 7; i >= 0; i--) {
            array[i] = (byte) (b & 1);
            b = (byte) (b >> 1);
        }

        for (byte b1 : array) {
            System.out.print(b1);
            System.out.print(" ");
        }
    }

    public static int getIndex(int num) {
        return num >> 3;
    }

    public static void add(byte[][] bits, int index, int num) {
        bits[index][getIndex(num)] |= 1 << getPosition(num);
    }

    public static int getPosition(int num) {
        return num & 0x07;
    }
}
