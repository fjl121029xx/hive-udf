package com.hll.dw;

//import com.alibaba.fastjson.JSON;
//import com.alibaba.fastjson.JSONArray;
//import com.alibaba.fastjson.JSONObject;

import org.json.JSONArray;
import org.json.JSONObject;

public class TestMain {

    private static boolean jsonContain(JSONObject textJson, String key) {

        return textJson.keySet().contains(key);
    }

    public static void main(String[] args) {

        String s = "{\"detail\":[{\"basketName\":\"1号篮子\",\"boxNum\":0.0,\"boxPrice\":0.0,\"discount\":0.0,\"foodID\":\"\",\"foodName\":\"土豆\",\"hllFoodName\":\"\",\"hllUnit\":\"\",\"isSFDetail\":0,\"isSetFood\":0,\"platformFoodID\":\"52197793\",\"price\":2.0,\"quantity\":1,\"sku_id\":\"\",\"specs\":[]},{\"basketName\":\"1号篮子\",\"boxNum\":0.0,\"boxPrice\":0.0,\"discount\":0.0,\"foodID\":\"\",\"foodName\":\"川香鸡柳\",\"hllFoodName\":\"\",\"hllUnit\":\"\",\"isSFDetail\":0,\"isSetFood\":0,\"platformFoodID\":\"52266382\",\"price\":2.0,\"quantity\":2,\"sku_id\":\"\",\"specs\":[]},{\"basketName\":\"1号篮子\",\"boxNum\":0.0,\"boxPrice\":0.0,\"discount\":0.0,\"foodID\":\"\",\"foodName\":\"夹心蟹味排\",\"hllFoodName\":\"\",\"hllUnit\":\"\",\"isSFDetail\":0,\"isSetFood\":0,\"platformFoodID\":\"52267416\",\"price\":2.0,\"quantity\":1,\"sku_id\":\"\",\"specs\":[]},{\"basketName\":\"1号篮子\",\"boxNum\":0.0,\"boxPrice\":0.0,\"discount\":0.0,\"foodID\":\"\",\"foodName\":\"油豆腐\",\"hllFoodName\":\"\",\"hllUnit\":\"\",\"isSFDetail\":0,\"isSetFood\":0,\"platformFoodID\":\"52202893\",\"price\":2.0,\"quantity\":1,\"sku_id\":\"\",\"specs\":[]},{\"basketName\":\"1号篮子\",\"boxNum\":0.0,\"boxPrice\":0.0,\"discount\":0.0,\"foodID\":\"\",\"foodName\":\"面筋泡\",\"hllFoodName\":\"\",\"hllUnit\":\"\",\"isSFDetail\":0,\"isSetFood\":0,\"platformFoodID\":\"52199916\",\"price\":2.0,\"quantity\":2,\"sku_id\":\"\",\"specs\":[]},{\"basketName\":\"1号篮子\",\"boxNum\":0.0,\"boxPrice\":0.0,\"discount\":0.0,\"foodID\":\"\",\"foodName\":\"微辣打包盒\",\"hllFoodName\":\"\",\"hllUnit\":\"\",\"isSFDetail\":0,\"isSetFood\":0,\"platformFoodID\":\"52194027\",\"price\":1.0,\"quantity\":1,\"sku_id\":\"\",\"specs\":[]},{\"basketName\":\"1号篮子\",\"boxNum\":0.0,\"boxPrice\":0.0,\"discount\":0.0,\"foodID\":\"\",\"foodName\":\"正常全选\",\"hllFoodName\":\"\",\"hllUnit\":\"\",\"isSFDetail\":0,\"isSetFood\":0,\"platformFoodID\":\"52194800\",\"price\":0.0,\"quantity\":1,\"sku_id\":\"\",\"specs\":[]},{\"basketName\":\"1号篮子\",\"boxNum\":0.0,\"boxPrice\":0.0,\"discount\":0.0,\"foodID\":\"\",\"foodName\":\"米线\",\"hllFoodName\":\"\",\"hllUnit\":\"\",\"isSFDetail\":0,\"isSetFood\":0,\"platformFoodID\":\"85484709\",\"price\":2.0,\"quantity\":1,\"sku_id\":\"\",\"specs\":[]},{\"basketName\":\"1号篮子\",\"boxNum\":0.0,\"boxPrice\":0.0,\"discount\":0.0,\"foodID\":\"\",\"foodName\":\"甜不辣\",\"hllFoodName\":\"\",\"hllUnit\":\"\",\"isSFDetail\":0,\"isSetFood\":0,\"platformFoodID\":\"55436586\",\"price\":2.0,\"quantity\":1,\"sku_id\":\"\",\"specs\":[]},{\"basketName\":\"1号篮子\",\"boxNum\":0.0,\"boxPrice\":0.0,\"discount\":0.0,\"foodID\":\"\",\"foodName\":\"鱼皮豆腐\",\"hllFoodName\":\"\",\"hllUnit\":\"\",\"isSFDetail\":0,\"isSetFood\":0,\"platformFoodID\":\"52266020\",\"price\":2.0,\"quantity\":1,\"sku_id\":\"\",\"specs\":[]},{\"basketName\":\"1号篮子\",\"boxNum\":0.0,\"boxPrice\":0.0,\"discount\":0.0,\"foodID\":\"\",\"foodName\":\"兰花千\",\"hllFoodName\":\"\",\"hllUnit\":\"\",\"isSFDetail\":0,\"isSetFood\":0,\"platformFoodID\":\"52200356\",\"price\":2.0,\"quantity\":1,\"sku_id\":\"\",\"specs\":[]},{\"basketName\":\"1号篮子\",\"boxNum\":0.0,\"boxPrice\":0.0,\"discount\":0.0,\"foodID\":\"\",\"foodName\":\"小青菜\",\"hllFoodName\":\"\",\"hllUnit\":\"\",\"isSFDetail\":0,\"isSetFood\":0,\"platformFoodID\":\"52195557\",\"price\":2.0,\"quantity\":1,\"sku_id\":\"\",\"specs\":[]},{\"basketName\":\"1号篮子\",\"boxNum\":0.0,\"boxPrice\":0.0,\"discount\":0.0,\"foodID\":\"\",\"foodName\":\"里脊肉\",\"hllFoodName\":\"\",\"hllUnit\":\"\",\"isSFDetail\":0,\"isSetFood\":0,\"platformFoodID\":\"52267554\",\"price\":2.0,\"quantity\":1,\"sku_id\":\"\",\"specs\":[]},{\"basketName\":\"1号篮子\",\"boxNum\":0.0,\"boxPrice\":0.0,\"discount\":0.0,\"foodID\":\"\",\"foodName\":\"冬瓜\",\"hllFoodName\":\"\",\"hllUnit\":\"\",\"isSFDetail\":0,\"isSetFood\":0,\"platformFoodID\":\"52199095\",\"price\":2.0,\"quantity\":1,\"sku_id\":\"\",\"specs\":[]}]}";
        String[] field = new String[]{"foodID", "foodName", "price", "quantity", "unit"};
        String[] fieldrr = new String[]{"specs"};

        JSONObject jsonObject = new JSONObject(s);
//        System.out.println(jsonObject);
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

        System.out.println(allAp.toString().substring(0, allAp.length() - 1));
    }

    private static String arrayMkstring(JSONArray arr) {

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
