package person.nightrunner.helper.flink.sql;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;

import java.util.Map;

import static person.nightrunner.helper.flink.sql.Constants.DEFAULT_TABLE_NAME;

public class DMLHelper {
  public static String getSelect(String json) {
    return "SELECT \n" + get(JSONObject.parseObject(json), null) + " \nFROM " + DEFAULT_TABLE_NAME;
  }

  private static String get(JSONObject jsonObject, String prefix) {
    StringBuilder builder = new StringBuilder();

    return String.join(",\n",jsonObject.keySet());

//    for (Map.Entry<String, Object> entry : jsonObject.entrySet()) {
//      String key = entry.getKey();
//        if (prefix != null) {
//          builder.append(prefix + ".");
//        }
//        builder.append(key);
//        builder.append(",\n");
//    }
//
//    return builder.toString();
  }

}
