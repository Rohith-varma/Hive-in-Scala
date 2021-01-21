package ca.rohith.bigdata.hive

import java.sql.DriverManager

object HiveClient extends App {
  val driverName : String = "org.apache.hive.jdbc.HiveDriver"
  Class.forName(driverName) // this step loads the JDBC to JVM
  val ConnectionString: String = "jdbc:hive2://quickstart.cloudera:10000/winter2020_rohith;user=rohith;"
  val connection = DriverManager.getConnection(ConnectionString) //creates JDBC connection
  val query = connection.createStatement()

  query.executeUpdate("DROP TABLE IF EXISTS winter2020_rohith.ext_trips")
  query.executeUpdate("""CREATE EXTERNAL TABLE IF NOT EXISTS winter2020_rohith.ext_trips(
                        |route_id                int,
                        |service_id              string,
                        |trip_id                 string,
                        |trip_headsign           string,
                        |direction_id            int,
                        |shape_id                int,
                        |wheelchair_accessible   int,
                        |note_fr                 string,
                        |note_en                 string
                        |)
                        |ROW FORMAT DELIMITED
                        |FIELDS TERMINATED BY ','
                        |STORED AS TEXTFILE
                        |LOCATION '/user/winter2020/rohith/project4/trips/'
                        |TBLPROPERTIES('skip.header.line.count'='1')""".stripMargin)

  query.executeUpdate("DROP TABLE IF EXISTS winter2020_rohith.ext_calendar_dates")
  query.executeUpdate("""CREATE EXTERNAL TABLE IF NOT EXISTS winter2020_rohith.ext_calendar_dates(
                        |service_id string,
                        |date string,
                        |exception_type int
                        |)
                        |ROW FORMAT DELIMITED
                        |FIELDS TERMINATED BY ','
                        |STORED AS TEXTFILE
                        |LOCATION '/user/winter2020/rohith/project4/calendar_dates/'
                        |TBLPROPERTIES('skip.header.line.count'='1')""".stripMargin)

  query.executeUpdate("DROP TABLE IF EXISTS winter2020_rohith.ext_frequencies")
  query.executeUpdate("""CREATE EXTERNAL TABLE IF NOT EXISTS winter2020_rohith.ext_frequencies(
                        |trip_id         string,
                        |start_time      string,
                        |end_time        string,
                        |headway_secs    int
                        |)
                        |ROW FORMAT DELIMITED
                        |FIELDS TERMINATED BY ','
                        |STORED AS TEXTFILE
                        |LOCATION '/user/winter2020/rohith/project4/frequencies/'
                        |TBLPROPERTIES('skip.header.line.count'='1')""".stripMargin)

  query.executeUpdate("set hive.exec.dynamic.partition = true")
  query.executeUpdate("set hive.exec.dynamic.partition.mode = nonstrict")

  query.executeUpdate("""INSERT OVERWRITE TABLE winter2020_rohith.enriched_trip PARTITION(wheelchair_accessible)
                        |SELECT t.route_id,t.service_id,t.trip_id,t.trip_headsign,t.direction_id,f.start_time,
                        |f.end_time,cd.exception_type,t.wheelchair_accessible FROM
                        |winter2020_rohith.ext_trips t LEFT JOIN winter2020_rohith.ext_calendar_dates cd
                        |ON t.service_id = cd.service_id LEFT JOIN winter2020_rohith.ext_frequencies f
                        |ON t.trip_id=f.trip_id""".stripMargin)
  query.executeUpdate("MSCK REPAIR table winter2020_rohith.enriched_trip")

  query.close()
  connection.close()
}

