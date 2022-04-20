import org.apache.spark.SparkContext
import java.io.File


package object imdb {
  val STORE_PATH = "src/main/resources/results/"
  abstract sealed class Record extends Product with Serializable
  case class Undefined() extends Record
  case class Aka_name (
                        id   : Int,
                        person_id   : Int,
                        name   : String,
                        imdb_index   : String,
                        name_pcode_cf   : String,
                        name_pcode_nf   : String,
                        surname_pcode   : String,
                        md5sum   : String
                      )  extends Record

  case class Aka_title (
                         id   : Int,
                         movie_id   : Int,
                         title   : String   ,
                         imdb_index   : String,
                         kind_id   : Int,
                         production_year   : Int,
                         phonetic_code   : String,
                         episode_of_id   : Int,
                         season_nr   : Int,
                         episode_nr   : Int,
                         note   : String,
                         md5sum   : String
                       )  extends Record

  case class Cast_info (
                         id   : Int,
                         person_id   : Int,
                         movie_id   : Int,
                         person_role_id   : Int,
                         note   : String,
                         nr_order   : Int,
                         role_id   : Int
                       )  extends Record

  case class Char_name (
                         id   : Int,
                         name   : String,
                         imdb_index   : String,
                         imdb_id   : Int,
                         name_pcode_nf   : String,
                         surname_pcode   : String,
                         md5sum   : String
                       )  extends Record

  case class Comp_cast_type (
                              id   : Int,
                              kind   : String
                            )  extends Record

  case class Company_name (
                            id   : Int,
                            name   : String,
                            country_code   : String,
                            imdb_id   : Int,
                            name_pcode_nf   : String,
                            name_pcode_sf   : String,
                            md5sum   : String
                          )  extends Record

  case class Company_type (
                            id   : Int,
                            kind   : String
                          )  extends Record

  case class Complete_cast (
                             id   : Int,
                             movie_id   : Int,
                             subject_id   : Int,
                             status_id   : Int
                           )  extends Record

  case class Info_type (
                         id   : Int,
                         info   : String
                       )  extends Record

  case class Keyword (
                       id   : Int,
                       word   : String,
                       phonetic_code   : String
                     )  extends Record

  case class Kind_type (
                         id   : Int,
                         kind   : String
                       )  extends Record

  case class Link_type (
                         id   : Int,
                         link   : String
                       )  extends Record

  case class Movie_companies (
                               id   : Int,
                               movie_id   : Int,
                               company_id   : Int,
                               company_type_id   : Int,
                               note   : String
                             )  extends Record

  case class Movie_info_idx (
                              id   : Int,
                              movie_id   : Int,
                              info_type_id   : Int,
                              info   : String,
                              note   : String
                            )  extends Record

  case class Movie_keyword (
                             id   : Int,
                             movie_id   : Int,
                             word_id   : Int
                           )  extends Record

  case class Movie_link (
                          id   : Int,
                          movie_id   : Int,
                          linked_movie_id   : Int,
                          link_type_id   : Int
                        )  extends Record

  case class Name (
                    id   : Int,
                    name   : String,
                    imdb_index   : String,
                    imdb_id   : Int,
                    gender   : String,
                    name_pcode_cf   : String,
                    name_pcode_nf   : String,
                    surname_pcode   : String,
                    md5sum   : String
                  )  extends Record

  case class Role_type (
                         id   : Int,
                         role   : String
                       )  extends Record

  case class Title (
                     id   : Int,
                     title   : String,
                     imdb_index   : String,
                     kind_id   : Int,
                     production_year   : Int,
                     imdb_id   : Int,
                     phonetic_code   : String,
                     episode_of_id   : Int,
                     season_nr   : Int,
                     episode_nr   : Int,
                     series_years   : String,
                     md5sum   : String
                   )  extends Record

  case class Movie_info (
                          id   : Int,
                          movie_id   : Int,
                          info_type_id   : Int,
                          info   : String,
                          note   : String
                        )  extends Record

  case class Person_info (
                           id   : Int,
                           person_id   : Int,
                           info_type_id   : Int,
                           info   : String,
                           note   : String
                         )  extends Record

//  case "aka_name" =>
//case "aka_title" =>
//case "cast_info" =>
//case "char_name" =>
//case "comp_cast_type" =>
//case "company_name" =>
//case "company_type" =>
//case "complete_cast" =>
//case "info_type" =>
//case "keyword" =>
//case "kind_type" =>
//case "link_type" =>
//case "movie_companies" =>
//case "movie_info_idx" =>
//case "movie_keyword" =>
//case "movie_link" =>
//case "name" =>
//case "role_type" =>
//case "title" =>
//case "movie_info" =>
//case "person_info" =>
def timingInMs(f : ()=>List[Any]) : (List[Any], Double) = {
  val start = System.nanoTime()
  val output = f()
  val end = System.nanoTime()
  (output, (end-start)/1000000.0)
}

  def min_s(a: String, b : String) : String = List(a,b).min

  def id_toInt(s: String): Option[Int] = {
    try {
      Some(s.toInt)
    } catch {
      case e: Exception => None
    }
  }
  def toInt(s: String): Int = {
    try{
      s.toInt
    } catch{
      case e: Exception => -1
    }
  }
  private def get_record(cols : Array[String], name : String): Option[Record] = {
    id_toInt(cols(0)) match {
      case Some(_) => name match {
        case "aka_name" =>Some(Aka_name(cols(0).toInt, cols(1).toInt, cols(2), cols(3), cols(4), cols(5), cols(6), cols(7)))
        case "aka_title" =>Some(Aka_title(cols(0).toInt, cols(1).toInt, cols(2), cols(3), cols(4).toInt, cols(5).toInt, cols(6), cols(7).toInt, cols(8).toInt, cols(9).toInt, cols(10), cols(11)))
        case "cast_info" =>Some(Cast_info(cols(0).toInt, cols(1).toInt, cols(2).toInt, cols(3).toInt, cols(4), cols(5).toInt, cols(6).toInt))
        case "char_name" =>Some(Char_name(cols(0).toInt, cols(1), cols(2), cols(3).toInt, cols(4), cols(5),cols(6)))
        case "comp_cast_type" =>Some(Comp_cast_type(cols(0).toInt, cols(1)))
        case "company_name" =>Some(Company_name(cols(0).toInt, cols(1), cols(2), cols(3).toInt, cols(4), cols(5), cols(6)))
        case "company_type" =>Some(Company_type(cols(0).toInt, cols(1)))
        case "complete_cast" =>Some(Complete_cast(cols(0).toInt, cols(1).toInt, cols(2).toInt, cols(3).toInt))
        case "info_type" =>Some(Info_type(cols(0).toInt, cols(1)))
        case "keyword" =>Some(Keyword(cols(0).toInt, cols(1), cols(2)))
        case "kind_type" =>Some(Kind_type(cols(0).toInt, cols(1)))
        case "link_type" =>Some(Link_type(cols(0).toInt, cols(1)))
        case "movie_companies" =>Some(Movie_companies(cols(0).toInt, cols(1).toInt, cols(2).toInt, cols(3).toInt, cols(4)))
        case "movie_info_idx" =>Some(Movie_info_idx(cols(0).toInt, cols(1).toInt, cols(2).toInt, cols(3), cols(4)))
        case "movie_keyword" =>Some(Movie_keyword(cols(0).toInt, cols(1).toInt, cols(2).toInt))
        case "movie_link" =>Some(Movie_link(cols(0).toInt, cols(1).toInt, cols(2).toInt, cols(3).toInt))
        case "name" =>Some(Name(cols(0).toInt, cols(1), cols(2), cols(3).toInt, cols(4), cols(5), cols(6), cols(7), cols(8)))
        case "role_type" =>Some(Role_type(cols(0).toInt, cols(1)))
        case "title" =>Some(Title(cols(0).toInt, cols(1), cols(2), toInt(cols(3)), toInt(cols(4)), toInt(cols(5)), cols(6), toInt(cols(7)), toInt(cols(8)), toInt(cols(9)), cols(10), cols(11)))
        case "movie_info" =>Some(Movie_info(cols(0).toInt, cols(1).toInt, cols(2).toInt, cols(3), cols(4)))
        case "person_info" =>Some(Person_info(cols(0).toInt, cols(1).toInt, cols(2).toInt, cols(3), cols(4)))

        case _ => None
      }
      case None => None
    }
  }

  private def get_default_record(name : String): Record = {
//    name match {
//        case "aka_name" =>Aka_name(-1, -1,"","","","","","")
//      case "aka_title" =>Aka_title( -1, -1, "", "", -1, -1, "", -1, -1, -1, "", "")
//      case "cast_info" =>Cast_info(-1, -1,-1,-1,"",-1,-1)
//      case "char_name" => Char_name(-1, "", "", -1 ,"" ,"" ,"")
//      case "comp_cast_type" => Comp_cast_type(-1, "")
//      case "company_name" =>Company_name(-1, "", "", -1, "", "", "")
//      case "company_type" =>Company_type(-1, "")
//      case "complete_cast" =>Complete_cast(-1, -1, -1, -1)
//      case "info_type" =>Info_type(-1, "")
//      case "keyword" =>Keyword(-1, "", "")
//      case "kind_type" =>Kind_type(-1, "")
//      case "link_type" =>Link_type(-1, "")
//      case "movie_companies" =>Movie_companies(-1, -1, -1, -1, "")
//      case "movie_info_idx" =>Movie_info_idx(-1, -1, -1, "", "")
//      case "movie_keyword" => Movie_keyword(-1, -1, -1)
//      case "movie_link" =>Movie_link(-1, -1, -1, -1)
//      case "name" =>Name(-1, "", "", -1, "", "", "", "", "")
//      case "role_type" => Role_type(-1, "")
//      case "title" =>Title(-1, "", "", -1, -1, -1, "", -1, -1, -1, "", "")
//      case "movie_info" =>Movie_info(-1, -1, -1, "", "")
//      case "person_info" =>Person_info(-1, -1, -1, "", "")
//        case _ => Undefined()
//    }
    Undefined()
  }
  def getPath(name: String, folder: String): String = {
    new File(getClass.getResource("/" + folder + "/" + name +".csv").getFile).getPath
  }
  def load(sc : SparkContext, name : String, numPartitions: Int): org.apache.spark.rdd.RDD[Record] = {
    load(sc, name).repartition(numPartitions)
  }

  def load(sc : SparkContext, name : String): org.apache.spark.rdd.RDD[Record] = {
    val path = getPath(name, "imdb")
    val file = sc.textFile(path)
    val data = file
      .map(l => {
        val cols = l.split(",", -1)
          .map(_.trim)
        get_record(cols, name)

      })
      .filter({ case Some(_) => true
      case None => false
      })
      .map({ case Some(x) => x
      case None => get_default_record(name)
      })
    data.filter {
      case Undefined() => false
      case _ => true
    }
  }
}
