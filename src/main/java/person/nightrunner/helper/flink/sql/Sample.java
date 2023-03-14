package person.nightrunner.helper.flink.sql;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;

public class Sample {

  public static void main(String[] args) throws URISyntaxException, IOException {
    String content =
        Files.readString(
            Path.of(ClassLoader.getSystemClassLoader().getResource("input.properties").toURI()));
    System.out.println(DDLHelper.getCreateTable(content));
//    System.out.println(DMLHelper.getSelect(content));
  }
}
