package person.nightrunner.helper.flink.sql;

import com.alibaba.fastjson2.JSONObject;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class FlinkSqlGenerator {

  public static void main(String[] args) {
    String content;
    try {
      content =
          Files.readString(
              Path.of(ClassLoader.getSystemClassLoader().getResource("input.properties").toURI()));
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }

    System.out.println(byJson(content));
  }

  public static String byJson(String json) {
    return "CREATE TABLE table_name AS (\n" + convert(json, true) + "\n) WITH (  );";
  }

  private static String convert(com.alibaba.fastjson2.JSONObject jsonObject, boolean fileNewLine) {

    Set<String> keys = jsonObject.keySet();
    List<NameValue> nameValueList = new ArrayList<>();

    for (String key : keys) {
      NameValue nameValue = new NameValue();
      nameValue.name = key;
      nameValue.value = jsonObject.get(key);
      nameValueList.add(nameValue);
    }

    StringBuilder builder = new StringBuilder();

    for (int i = 0; i < nameValueList.size(); i++) {
      NameValue nameValue = nameValueList.get(i);
      builder.append("`");
      builder.append(nameValue.name);
      builder.append("`");
      builder.append(" ");

      TypeConvert typeConvert = TypeConvert.of(nameValue.value);

      if (TypeConvert.Complex.equals(typeConvert)) {
        builder.append("<ROW ");
        builder.append(convert((com.alibaba.fastjson2.JSONObject) nameValue.value, false));
        builder.append(">,");
      } else if (TypeConvert.Array.equals(typeConvert)) {
        builder.append("ARRAY<ROW<");
        builder.append(
            convert(((com.alibaba.fastjson2.JSONArray) nameValue.value).getJSONObject(0), false));
        builder.append(">>,");
      } else {
        Optional<TypeConvert> optionalTypeConvert = Optional.ofNullable(typeConvert);
        builder.append(optionalTypeConvert.get().getFlinkSQLType());
        builder.append(",");
      }
      if (fileNewLine && i < nameValueList.size() - 1) {
        builder.append("\n");
      }
    }
    String s = builder.toString();
    if (s.endsWith(",")) {
      s = s.substring(0, s.length() - 1);
    }
    return s;
  }

  private static String convert(String json, boolean fieldNewLine) {
    return convert(JSONObject.parseObject(json), fieldNewLine);
  }
}

class NameValue {
  String name;
  Object value;
}

enum TypeConvert {
  Integer("java.lang.Integer", "INT"),
  Long("java.lang.Long", "BIGINT"),
  String("java.lang.String", "STRING"),
  DATETIME("java.lang.String", "DATETIME"),
  BigDecimal("java.math.BigDecimal", "BIGINT"),
  Complex("com.alibaba.fastjson2.JSONObject", "RAW"),

  Array("com.alibaba.fastjson2.JSONArray", "ARRAY");

  TypeConvert(String javaType, String flinkSQLType) {
    this.javaType = javaType;
    this.flinkSQLType = flinkSQLType;
  }

  public java.lang.String getJavaType() {
    return javaType;
  }

  public java.lang.String getFlinkSQLType() {
    return flinkSQLType;
  }

  private String javaType;

  private String flinkSQLType;

  public static TypeConvert of(Object value) {

    String[] formats = new String[] {"yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd"};

    if ("java.lang.String".equals(value.getClass().getName())) {
      for (String format : formats) {
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        try {
          sdf.parse(value.toString());
          return TypeConvert.DATETIME;
        } catch (ParseException e) {
          //                    System.out.println("not " + format);
          //          e.printStackTrace();
        }
      }
    }

    for (TypeConvert typeConvert : values()) {
      if (value.getClass().getName().equals(typeConvert.javaType)) {
        return typeConvert;
      }
    }
    return null;
  }
}
