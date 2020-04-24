package com.hll.udf.v2.dif;

import java.math.BigDecimal;

import static java.math.BigDecimal.ROUND_HALF_DOWN;

public class Demo {
    public static void main(String[] args) {

        BigDecimal startSecond = new BigDecimal(1530955370.0);
        BigDecimal endSecond = new BigDecimal(1530955402.0);
        BigDecimal div = new BigDecimal(3600000.0);
        BigDecimal result = endSecond.subtract(startSecond).divide(div, 2, ROUND_HALF_DOWN).setScale(2, BigDecimal.ROUND_UP);
        System.out.println(result);
//
    }
}
