package org.apache.parquet.proto;

import static org.apache.parquet.filter2.predicate.FilterApi.*;
import static org.junit.Assert.assertNotEquals;

import com.google.protobuf.Message;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.proto.test.TestProto3;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.FilterCompat.Filter;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.BeforeClass;
import org.junit.Test;


public class TestMultiRead {


  public static String COLUMN_NAME = "someId";
  static File file;
  static List<Message> entries = Stream.of(1, 2, 3, 4, 5, 6, 7, 8)
      .map(i -> TestProto3.SchemaConverterSimpleMessage.newBuilder().setSomeId(i).build())
      .collect(Collectors.toList());

  @BeforeClass
  public static void setup() throws IOException {
    // Create file with single column entries 1, 2, 3, 4...
    file = writeMessages(entries);
  }

  public static File writeMessages(List<Message> messages) throws IOException {

    File f = File.createTempFile("phonebook", ".parquet");
    f.deleteOnExit();
    if (!f.delete()) {
      throw new IOException("couldn't delete tmp file" + f);
    }

    int count = 0;
    if (messages.isEmpty()) return f;
    ProtoParquetWriter.Builder<Object> builder =
        ProtoParquetWriter.builder(new Path(f.getAbsolutePath()))
            .withMessage(messages.get(0).getClass())
            .withWriteMode(ParquetFileWriter.Mode.OVERWRITE);
    try (ParquetWriter<Object> writer = builder.build()) {
      for (Object message : messages) {
        writer.write(message);
        count++;
      }
    }
    System.out.println("count for write " + TestMultiRead.class + " is " + count);

    return f;
  }


  public static ParquetReader<Object> createReader(HadoopInputFile inputFile, Filter filter) throws IOException {

    return ProtoParquetReader.builder(inputFile)
        .withFilter(filter)
        .build();
  }

  /**
   * Get the last event from a file filtered by an input filter
   *
   * @param hadoopInputFile file
   * @param filter          filter for the file
   * @return last event before end and after start
   * @throws IOException if reading file fails
   */
  public static Object readFile(
      HadoopInputFile hadoopInputFile, FilterCompat.Filter filter)
      throws IOException {
    ParquetReader<Object> reader = createReader(hadoopInputFile, filter);
    int count = 0;
    Object value = reader.read();
    Object prevValue = value;
    if (value != null) {
      count = 1;
    }
    while (value != null) {
      prevValue = value;
      value = reader.read();
      count++;
    }
    System.out.println("count for " + TestMultiRead.class + " is " + count);
    return prevValue;
  }

  @Test
  public void testReadFileTwiceDifferentFilter() throws IOException {

    Path hadoopPath = new org.apache.hadoop.fs.Path(file.getAbsolutePath());
    Configuration config = new Configuration();

    HadoopInputFile hadoopInputFile = HadoopInputFile.fromPath(hadoopPath, config);

    // Create filters on the file
    FilterPredicate gtPredicate = gt(intColumn(COLUMN_NAME), 2);
    FilterCompat.Filter gtFilter = FilterCompat.get(gtPredicate);
    // Create another filter that is more restrictive
    FilterPredicate subPredicate = and(lt(intColumn(COLUMN_NAME), 5), gt(intColumn(COLUMN_NAME), 2));
    FilterCompat.Filter subFilter = FilterCompat.get(subPredicate);

    Object groupGt = readFile(hadoopInputFile, gtFilter);
    // Read file with another filter to not the end of the file
    Object groupSub = readFile(hadoopInputFile, subFilter);
    // show the last event read is now set to the event at the end of the file instead of end of filter
    assertNotEquals(groupGt, groupSub);
  }
}
