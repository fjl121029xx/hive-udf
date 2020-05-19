package com.hll.udaf.v5;

import java.math.BigDecimal;
import java.util.*;

public class ActiveStat {
    private List<String> add_up;
    private String dog;
    private boolean isValue;
    private byte[] current_bitMap;
    private byte[] current_bitMap2;
    private int current_index = 0;
    //
    private Set<String> keyList;
    private Set<String> idList;

    public ActiveStat(List<String> add_up, String dog, Set<String> keyList, Set<String> idList) {
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
        System.out.println("size " + add_up.size());

        int count = 8;
        int quotient = size / count;
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            int finalI = i;
            Thread thread = new Thread(() -> {
                try {
                    int end = (finalI + 1) * quotient;
                    int start = finalI * quotient;

                    if (end >= size && start <= size) {
                        fillBitMap(bits, keyMap, add_up.subList(start, size));
                        System.out.println("执行子线程 add_up.subList(" + start + ", " + size + ")");
                    } else {
                        fillBitMap(bits, keyMap, add_up.subList(start, end));
                        System.out.println("执行子线程 add_up.subList(" + start + ", " + end + ")");
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

        return countActive(dog, bits, dayNumMap, keyMap);
    }

    public Map<String, String> countActive(String dog, byte[][] bits, Map<Integer, String> dayNumMap, Map<String, Integer> kMap) {

        Map<String, String> returnMap = new HashMap<>();
        int dogInt = Integer.parseInt(dog);

        for (int i = 0; i < bits.length; i++) {
            int activeCount = 0;

            int afterDays = i + dogInt;
            byte[] b = formatByte(bits[i]);

            byte[] s;
            if (afterDays > bits.length - 1) {
                s = formatByte(new byte[bits[0].length]);
            } else {
                s = formatByte(bits[afterDays]);
            }

            for (int j = 0; j < b.length; j++) {
                if (b[j] == 1 && s[j] == 1) {
                    activeCount++;
                }
            }

            String day = dayNumMap.get(i);
            if (day != null) {
                if (isValue) {
                    returnMap.put(day, Integer.toString(activeCount));
                } else {
                    int sum_bit = sumBit(b);
                    BigDecimal n = new BigDecimal(activeCount);
                    BigDecimal m = new BigDecimal(sum_bit);
                    if (sum_bit != 0) {
                        returnMap.put(day, n.divide(m, 9, BigDecimal.ROUND_HALF_UP).toString());
                    } else {
                        returnMap.put(day, Double.toString(0.00));
                    }
                }

            }

        }


        return returnMap;
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
