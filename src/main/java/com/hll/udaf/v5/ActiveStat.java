package com.hll.udaf.v5;

import java.math.BigDecimal;
import java.util.*;

public class ActiveStat {
    private List<String> add_up;
    private String dog;
    private boolean isValue;

    public ActiveStat(List<String> add_up, String dog) {
        this.add_up = add_up;
        String[] split = dog.split("-");
        this.dog = split[0];
        this.isValue = split[1].equals("value");
    }

    public Map<String, String> compute() {

        Map<String, String> returnMap = new HashMap<>();

        Set<String> c = new HashSet<>();
        Set<String> v = new HashSet<>();

        for (String s : add_up) {
            String[] split = s.split(":");
            c.add(split[0]);
            v.add(split[1]);
        }

        byte[][] bits = new byte[c.size() + 1][getIndex(v.size()) + 1];

        Map<String, Integer> idMap = new HashMap<>();
        Map<String, Integer> keyMap = new HashMap<>();
        int k = 0;
        int l = 0;

        for (String en : add_up) {
            String pt = en.split(":")[0];
            String ufname = en.split(":")[1];

            if (keyMap.get(pt) == null) {
                keyMap.put(pt, l);
                l++;
            }
            if (idMap.get(ufname) == null) {
                idMap.put(ufname, k);
                k++;
            }
            Integer pt_index = keyMap.get(pt);
            Integer index = idMap.get(ufname);
            add(bits, pt_index, index);
        }

        // 1->日期
        Map<Integer, String> dayNumMap = new HashMap<>();
        for (String key : keyMap.keySet()) {
            dayNumMap.put(keyMap.get(key), key);
        }

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
