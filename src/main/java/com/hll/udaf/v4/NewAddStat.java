package com.hll.udaf.v4;

import java.math.BigDecimal;
import java.util.*;

public class NewAddStat {
    private List<String> add_up;
    private String dog;
    private boolean isValue;

    public NewAddStat(List<String> add_up, String dog) {
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

        return doByWhich(dog, bits, dayNumMap, keyMap);
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

    private byte[] getDayNewly(byte[][] bits, int dayIndex) {

        if (dayIndex == 0) {
            return formatByte(bits[0]);
        }

        int before = dayIndex - 1;

        byte[] before_bitmap = formatByte(new byte[bits[0].length]);
        for (int j = 0; j <= before; j++) {

            byte[] bit = formatByte(bits[j]);
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


    private Map<String, String> otherSave(String dog, byte[][] bits, Map<Integer, String> dayNumMap, Map<String, Integer> mmap) {

        Map<String, String> returnMap = new HashMap<>();

        int dogInt = Integer.parseInt(dog);
        for (int i = 0; i < mmap.size(); i++) {

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
        byte[] array = new byte[8 * b.length];
        for (int j = 0; j < b.length; j++) {
            for (int i = (7 + j * 8); i >= (0 + j * 8); i--) {
                array[i] = (byte) (b[j] & 1);
                b[j] = (byte) (b[j] >> 1);
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
