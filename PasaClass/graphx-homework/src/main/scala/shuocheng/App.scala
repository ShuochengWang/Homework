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


    val vertex_rdd: VertexRDD[List[Long]] = graph.aggregateMessages[List[Long]](
      // Map Function
      triplet => {
        triplet.sendToSrc(List(triplet.dstId))
      },
      // Reduce Function
      (a, b) => a ++ b
    )

    val sort_vertex_rdd = vertex_rdd.map(row => (row._1, row._2.sorted)).join(graph.vertices)
    val default_vertex_property = (List(), ("Missing", "Missing"))
    val graph2 = Graph(sort_vertex_rdd, graph.edges, default_vertex_property)

    def sorted_list_intersection(a: List[Long], b: List[Long]): List[Long] ={
      if(a == null || b == null) return List[Long]()
      var ap = 0
      var bp = 0
      var res = List[Long]()
      while(ap < a.length && bp < b.length){
        if(a(ap) == b(bp)){
          res = a(ap) :: res
          ap += 1
          bp += 1
        }
        else if(a(ap) < b(bp)){
          ap += 1
        }
        else{
          bp += 1
        }
      }
      res
    }

    val graph3 = graph2.mapTriplets(t => (t.attr, sorted_list_intersection(t.srcAttr._1, t.dstAttr._1)))
    graph3.edges.coalesce(1,true).saveAsTextFile("common-friends-result")

  }
}
