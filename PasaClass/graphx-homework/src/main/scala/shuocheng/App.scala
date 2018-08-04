package shuocheng

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession

object App {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local")
      .appName("graphx homework")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val airports_schema = StructType(
      StructField("AirportID", LongType) ::
        StructField("Name", StringType) ::
        StructField("City", StringType) ::
        StructField("Country", StringType) ::
        StructField("IATA", StringType) ::
        StructField("ICAO", StringType) ::
        StructField("Latitude", DoubleType) ::
        StructField("Longitude", DoubleType) ::
        StructField("Altitude", IntegerType) ::
        StructField("Timezone", StringType) ::
        StructField("DST", StringType) ::
        StructField("tzTimezone ", StringType) ::
        StructField("Type", StringType) ::
        StructField("Source", StringType) :: Nil)

    val routes_schema = StructType(
      StructField("Airline", StringType) ::
        StructField("AirlineID", IntegerType) ::
        StructField("SourceAirport", StringType) ::
        StructField("SourceAirportID", LongType) ::
        StructField("DestinationAirport", StringType) ::
        StructField("DestinationAirportID", LongType) ::
        StructField("Codeshare", StringType) ::
        StructField("Stops", IntegerType) ::
        StructField("Equipment", StringType) :: Nil)

    val airports_df = spark.read.schema(airports_schema).option("nullValue", "\\N").csv("dataset/airports.dat")
      .select("AirportID", "Name", "Country")
    val routes_df = spark.read.schema(routes_schema).option("nullValue", "\\N").csv("dataset/routes.dat")
        .select("SourceAirportID", "DestinationAirportID")
        .filter("SourceAirportID is not null and DestinationAirportID is not null")

    val edges = routes_df.groupBy("SourceAirportID", "DestinationAirportID")
      .count()
      .toDF("srcID", "dstID", "weight")

    val airport_vertices: RDD[(VertexId, (String, String))] = airports_df.rdd
        .map(row => (row.getLong(0), (row.getString(1), row.getString(2))))

    val routes_edges: RDD[Edge[Integer]] = edges.rdd
      .map(row => Edge(row.getLong(0), row.getLong(1), row.getLong(2).intValue()))

    val default_airport = ("Missing", "Missing")
    val graph = Graph(airport_vertices, routes_edges, default_airport)

    val ranks = graph.pageRank(0.0001).vertices

    val ranks_and_airports = airport_vertices.join(ranks)
      .persist()

    val china_ranks = ranks_and_airports.filter(_._2._1._2 == "China")
      .sortBy(_._2._2, ascending=false).persist()

    println("China Top 10:")
    china_ranks.take(10)
      .foreach(println)

    println("United States Top 10:")
    ranks_and_airports.filter(_._2._1._2 == "United States")
      .sortBy(_._2._2, ascending=false)
      .take(10)
      .foreach(println)

    val nanjing_airport_rank = ranks_and_airports.filter(_._2._1._1 == "Nanjing Lukou Airport").first()._2._2
    val rank_in_world = ranks_and_airports.filter(_._2._2 > nanjing_airport_rank).count() + 1
    val rank_in_china = china_ranks.filter(_._2._2 > nanjing_airport_rank).count() + 1
    println("rank in the world: " + rank_in_world)
    println("rank in China: " + rank_in_china)

  }
}
