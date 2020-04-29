package com.hll.udaf.v4;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NewAddStat {
    private List<String> add_up;

    public NewAddStat(List<String> add_up) {
        this.add_up = add_up;
    }

    public Map<String, String> compute() {

        Map<String, String> returnMap = new HashMap<>();

        byte[][] bits = new byte[add_up.size()][getIndex(add_up.size()) + 1];

        Map<String, Integer> dmap = new HashMap<>();
        Map<String, Integer> mmap = new HashMap<>();
        int k = 0;
        int l = 0;

        for (String en : add_up) {
            String pt = en.split(":")[0];
            String ufname = en.split(":")[1];

            if (dmap.get(ufname) == null) {
                dmap.put(ufname, k);
                k++;
            }
            if (mmap.get(pt) == null) {
                mmap.put(pt, l);
                l++;
            }
            Integer pt_index = mmap.get(pt);
            Integer index = dmap.get(ufname);
            add(bits, pt_index, index);
        }

        Map<Integer, String> re_mmap = new HashMap<>();
        for (String key : mmap.keySet()) {
            re_mmap.put(mmap.get(key), key);

        }
        byte[] first = formatByte(bits[0]);

        int count = 0;
        for (byte b : first) {
            if (b == 1) {
                count++;
            }
        }
        returnMap.put(re_mmap.get(0), Integer.toString(count));

        for (int i = 0; i < mmap.size(); i++) {
            int cal = calculate(i, i + 1, bits, re_mmap);
            String s = re_mmap.get(i + 1);
            if (s != null) {
                returnMap.put(s, Integer.toString(cal));
            }
        }
        return returnMap;
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
        int newAdd = 0;

        byte[] bitnext_tmp = formatByte(new byte[bits[from].length]);

        for (int j = 0; j < from; j++) {

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
        int[] sad = new int[a.length];

        for (int i = 0; i < a.length; i++) {
            byte s = a[i];
            byte e = b[i];
            int tmp = s ^ e;
            if (s == 0 && tmp == 1) {
                newAdd++;
            }
            sad[i] = tmp;
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
