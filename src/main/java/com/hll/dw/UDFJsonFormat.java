package com.hll.dw;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.log4j.Logger;

import org.json.JSONArray;
import org.json.JSONObject;

@Description(name = "UDFJsonFormat",
        value = "_FUNC_(String) - " +
                "text:String")
public class UDFJsonFormat extends UDF {
    public static Logger logger = Logger.getLogger(UDFJsonFormat.class);

    public String evaluate(String text) throws UDFArgumentException {

        try {
            String[] field = new String[]{"foodID", "foodName", "price", "quantity", "unit"};
            String[] fieldrr = new String[]{"specs"};
            JSONObject jsonObject = new JSONObject(text);

            StringBuilder allAp = new StringBuilder();
            JSONArray detail = jsonObject.getJSONArray("detail");
            for (int i = 0; i < detail.length(); i++) {
                JSONObject textJson = detail.getJSONObject(i);
                StringBuilder ap = new StringBuilder();

                for (String f : field) {
                    String a = jsonContain(textJson, f) ? f + ":" + textJson.getString(f) : "";
                    if (!a.equals("")) {
                        ap.append(a).append(",");
                    }
                }
                for (String f : fieldrr) {

                    String a = jsonContain(textJson, f) ? f + ":" + arrayMkstring(textJson.getJSONArray(f)) : "";
                    if (!a.equals("")) {
                        ap.append(a).append(",");
                    }
                }

                allAp.append(ap.toString().substring(0, ap.length() - 1)).append("/");
            }
            return allAp.toString().substring(0, allAp.length() - 1);
        } catch (Exception e) {
            logger.info("！！！！！！！！！！！！！！！！！！！！ " + text);
            e.printStackTrace();
            return text;
        }
    }


    public boolean jsonContain(JSONObject textJson, String key) {
        return textJson.keySet().contains(key);
    }

    public String arrayMkstring(JSONArray arr) {

        if (arr.length() == 0) {
            return "()";
        }
        StringBuilder ap = new StringBuilder();
        for (int i = 0; i < arr.length(); i++) {
            ap.append(arr.getString(i)).append(",");
        }

        return "(" + ap.toString().substring(0, ap.length() - 1) + ")";

    }
}
