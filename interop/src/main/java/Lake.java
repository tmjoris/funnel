import org.apache.spark.sql.*;
public class Lake {
  public static void main(String[] a) {
    String mode = a[0], dir = a[1];
    SparkSession.Builder b = SparkSession.builder().master("local[2]").appName("lake-" + mode)
      .config("spark.ui.enabled", "false")
      .config("spark.driver.bindAddress", "127.0.0.1").config("spark.driver.host", "127.0.0.1")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.lake.type", "jdbc")
      .config("spark.sql.catalog.lake.uri", "jdbc:sqlite:" + dir + "/catalog.db")
      .config("spark.sql.catalog.lake.warehouse", dir + "/warehouse")
      .config("spark.sql.ansi.enabled", "false")
      .config("spark.memory.offHeap.enabled", "true").config("spark.memory.offHeap.size", "2g")
      .config("spark.sql.shuffle.partitions", "4");
    if (mode.equals("comet")) b.config("spark.plugins", "org.apache.spark.CometPlugin")
      .config("spark.shuffle.manager", "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager");
    if (mode.equals("velox")) b.config("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .config("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager");
    SparkSession s = b.getOrCreate();
    for (String q : new String[]{
      "SELECT count(*) c, sum(sum) s, max(max) m FROM lake.funnel.metrics_1h WHERE tenant = 'acme' AND metric = 'http.requests'",
      "SELECT sum(count) c, sum(sum) s, min(min) lo, max(max) hi FROM lake.funnel.metrics WHERE tenant = 'acme' AND metric = 'queue.depth'"}) {
      Dataset<Row> d = s.sql(q);
      System.out.println("RESULT " + mode + " " + d.collectAsList());
      String plan = d.queryExecution().executedPlan().treeString();
      int i = plan.indexOf("== Initial Plan ==");
      String f = i < 0 ? plan : plan.substring(0, i);
      System.out.println("SCAN " + mode + " " + (f.contains("CometIcebergNativeScan") ? "comet-native" : f.contains("IcebergScanTransformer") ? "velox-native" : "jvm"));
    }
    s.stop();
  }
}
