package com.hll.test;

import java.util.List;
import java.util.Map;

public interface Udf {

    // iterate
    boolean iterate(Map<String, Map<String, String>> PartialResult, List<String> dimensions, List<String> measure, List<String> mathFunction);

    // terminatePartial
    Map<String, Map<String, String>> terminatePartial();

    // merge
    boolean merge(Map<String, Map<String, String>> PartialResult, Map<String, Map<String, String>> mapOutput);

    // terminate
    Map<String, String> terminate();
}
