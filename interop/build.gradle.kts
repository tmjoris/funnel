// Opens a Funnel lake from Spark, Comet or Velox through the standard Iceberg JDBC catalog.
//
//   gradle run -Pengine=jvm --args="jvm /path/to/lake"
//   gradle run -Pengine=comet --args="comet /path/to/lake"
//   gradle run -Pengine=velox -PglutenDir=/dir/with/gluten-jar --args="velox /path/to/lake"
plugins {
    java
    application
}

repositories { mavenCentral() }

java { toolchain { languageVersion.set(JavaLanguageVersion.of(21)) } }

val engine: String = (findProperty("engine") as String?) ?: "jvm"

dependencies {
    implementation("org.apache.spark:spark-sql_2.13:4.0.2")
    implementation("org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0")
    implementation("org.xerial:sqlite-jdbc:3.47.1.0")
    if (engine == "comet") implementation("org.apache.datafusion:comet-spark-spark4.0_2.13:1.2.0")
    if (engine == "velox") {
        implementation("org.apache.spark:spark-hive_2.13:4.0.2")
        implementation(fileTree(mapOf("dir" to (findProperty("glutenDir") as String), "include" to listOf("*.jar"))))
    }
}

application {
    mainClass.set("Lake")
    applicationDefaultJvmArgs = listOf(
        "-Xmx3g",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
        "--add-opens=java.base/java.io=ALL-UNNAMED",
        "--add-opens=java.base/java.net=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
        "-Dio.netty.tryReflectionSetAccessible=true",
    )
}
