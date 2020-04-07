package com.hll.udaf;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class Demo {
    public static void main(String[] args) throws ParseException {

//        2020-01-01 00:00:00	2020-01-08 00:00:00
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

        Date startTime = sdf.parse("2020-01-01");
        Date endTime = sdf.parse("2020-02-08");
        int i = 0;

//        if (startTime.getTime() > endTime.getTime()) {
//            Date tmp = startTime;
//            startTime = endTime;
//            endTime = tmp;
//            i = -1;
//        }
        Calendar startCa = Calendar.getInstance();
        startCa.setTime(startTime);
        Calendar endCa = Calendar.getInstance();
        endCa.setTime(endTime);

        LocalDate startDate = LocalDate.of(startCa.get(Calendar.YEAR), startCa.get(Calendar.MONTH) + 1, startCa.get(Calendar.DAY_OF_MONTH));
        LocalDate endDate = LocalDate.of(endCa.get(Calendar.YEAR), endCa.get(Calendar.MONTH) + 1, endCa.get(Calendar.DAY_OF_MONTH));
        System.out.println(ChronoUnit.WEEKS.between(startDate,endDate));
    }
}
