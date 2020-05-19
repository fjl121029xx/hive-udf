package com.hll.udaf.v4;

import java.math.BigDecimal;
import java.util.*;

public class NewAddStat {
    private List<String> add_up;
    private String dog;
    private boolean isValue;
    private byte[] last_bit_map;
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

    public void fillBitMap(byte[][] bits, Map<String, Integer> keyMap, List<String> add) {
        for (String en : add) {
            String key = en.split(":")[0];
            int index = Integer.parseInt(en.split(":")[1]);
            add(bits, keyMap.get(key), index);
        }
    }

    public Map<String, String> compute() throws InterruptedException {

        byte[][] bits = new byte[keyList.size() + 1][getIndex(idList.size()) + 1];

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

        long l1 = System.currentTimeMillis();
        int size = add_up.size();

        int count = 8;
        int quotient = size / count;
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            int finalI = i;
            Thread thread = new Thread(() -> {
                try {

                    if ((finalI + 1) * quotient >= add_up.size() && finalI * quotient <= add_up.size()) {
                        fillBitMap(bits, keyMap, add_up.subList(finalI * quotient, add_up.size()));
                        System.out.println("执行子线程 add_up.subList(" + (finalI * quotient) + ", " + add_up.size() + ")");
                    } else {
                        fillBitMap(bits, keyMap, add_up.subList(finalI * quotient, (finalI + 1) * quotient));
                        System.out.println("执行子线程 add_up.subList(" + (finalI * quotient) + ", " + ((finalI + 1) * quotient) + ")");
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            thread.start();
            threads.add(thread);
        }
        for (int i = 0; i < count; i++) {
            threads.get(i).join();
        }
        System.out.println("执行主线程");

        long l2 = System.currentTimeMillis();
        System.out.println("add 耗时 " + (l2 - l1));

        // 1->日期
        Map<Integer, String> dayNumMap = new HashMap<>();
        for (String key : keyMap.keySet()) {
            dayNumMap.put(keyMap.get(key), key);
        }
        long e = System.currentTimeMillis();
        System.out.println("反转耗时 " + (e - l2));

        Map<String, String> map = doByWhich(dog, bits, dayNumMap, keyMap);
        long end = System.currentTimeMillis();
        System.out.printf("compute time %d%n", (end - e));
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

    // 获得第dayIndex天的新增
    private byte[] getDayNewly(byte[][] bits, int dayIndex) {

        if (dayIndex == 0) {
            this.last_bit_map = formatByte(bits[0]);
            return formatByte(bits[0]);
        }

        int before = dayIndex - 1;

        byte[] before_bitmap = this.last_bit_map;
        if (before > 0) {
            byte[] bit = formatByte(bits[before]);
            for (int i = 0; i < before_bitmap.length; i++) {
                byte e = before_bitmap[i];
                byte s = bit[i];
                int tmp;
                if (e == 1 || s == 1) {
                    before_bitmap[i] = 1;
                } else {
                    before_bitmap[i] = 0;
                }
            }
            this.last_bit_map = before_bitmap;
        }

        byte[] bitmap = formatByte(bits[dayIndex]);

        byte[] result_bitmap = formatByte(new byte[bits[0].length]);
        for (int i = 0; i < before_bitmap.length; i++) {

            byte e = before_bitmap[i];
            byte s = bitmap[i];
            if (e == 0 && s == 1) {
                result_bitmap[i] = 1;
            }
        }
        return result_bitmap;
    }


    // 留存新增
    private Map<String, String> otherSave(String dog, byte[][] bits, Map<Integer, String> dayNumMap, Map<String, Integer> keyMap) {

        Map<String, String> returnMap = new HashMap<>();
        int dogInt = Integer.parseInt(dog);

        long l = System.currentTimeMillis();
        for (int i = 0; i < keyMap.size(); i++) {

            String s1 = dayNumMap.get(i);
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

            String s = s1;
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
        long l2 = System.currentTimeMillis();
        System.out.println("otherSave 耗时 " + (l2 - l));
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

    // 新增数
    public int calculate(int from, int to, byte[][] bits, Map<Integer, String> re_mmap) {

        byte[] bitnext_tmp;
        if (from == 0) {
            bitnext_tmp = formatByte(bits[0]);
            this.last_bit_map = bitnext_tmp;
        } else {
            byte[] bit = formatByte(bits[from]);
            for (int i = 0; i < this.last_bit_map.length; i++) {
                byte e = this.last_bit_map[i];
                byte s = bit[i];
                int tmp;
                if ((tmp = e | s) == 1) {
                    this.last_bit_map[i] = 1;
                } else {
                    this.last_bit_map[i] = 0;
                }
            }
        }


        byte[] a = this.last_bit_map;
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
