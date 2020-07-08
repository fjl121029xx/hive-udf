create function udf_row_col_stst as 'com.hll.udf.v4.UDFRowColStat' using jar 'hdfs://cluster/funs/hive-udf-4.0.jar';
create function udaf_row_col_stat as 'com.hll.udaf.v7.UDAFRowColStatistics' using jar 'hdfs://cluster/funs/hive-udf-4.0.jar';
create function udaf_adv_row_col_stat as 'com.hll.udf.v5.UDFAdvRowColStat' using jar 'hdfs://cluster/funs/hive-udf-4.0.jar';
create function udaf_adv_compute as 'com.hll.udaf.v7.UDAFAdvancedComputing' using jar 'hdfs://cluster/funs/hive-udf-4.0.jar';
create function mutil_date_format as 'com.hll.udf.v3.UDFMutilDateFormat' using jar 'hdfs://cluster/funs/hive-udf-4.0.jar';