package imdb
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

abstract class Query(sc: SparkContext, numPartitions: Int) {
  def execute(): (List[Any])
  def repartition(numPartitions: Int)
}


class Q3(sc: SparkContext, numPartitions: Int) extends Query(sc: SparkContext, numPartitions: Int) {
  var k: RDD[Keyword] = load(sc, "keyword", numPartitions).asInstanceOf[RDD[Keyword]].persist()
  var mi: RDD[Movie_info] = load(sc, "movie_info", numPartitions).asInstanceOf[RDD[Movie_info]].persist()
  var mk: RDD[Movie_keyword] = load(sc, "movie_keyword", numPartitions).asInstanceOf[RDD[Movie_keyword]].persist()
  var t: RDD[Title] = load(sc, "title", numPartitions).asInstanceOf[RDD[Title]].persist()
  k.takeSample(withReplacement = false,2).toList
  mi.takeSample(withReplacement = false,2).toList
  mk.takeSample(withReplacement = false,2).toList
  t.takeSample(withReplacement = false,2).toList

  override def repartition(numPartitions: Int): Unit = {
    k = k.repartition(numPartitions)
    mi = mi.repartition(numPartitions)
    mk = mk.repartition(numPartitions)
    t = t.repartition(numPartitions)
  }

  override def execute(): (List[Any]) = {
    val k_f = k.filter(_.word.contains("sequel")).map(_.id -> false)
    val mi_f = mi.filter(x => List("Sweden", "Norway", "Germany", "Denmark",
      "Swedish", "Denish", "Norwegian", "German").contains(x.info)).map(_.movie_id -> false)
    val t_f = t.filter(_.production_year > 2005).map(x => x.id -> x.title)
    val mk_f = mk.map(x => x.movie_id -> x.word_id)

    //t_id = mi_movie_id -> t.title
    val join1 = t_f.join(mi_f).map(x => x._1 -> x._2._1)
    //mk.word -> t.title
    val join2 = join1.join(mk_f).map(x => x._2._2 -> x._2._1)
    val table_res = join2.join(k_f).map(_._2._1)
    val res = table_res.reduce(min_s(_,_))

    List(res)
  }
}


//abstract class Query {
//  def execute(): (List[Any])
//}



//class Q1(ct: RDD[Company_type],
//         it: RDD[Info_type],
//         mc: RDD[Movie_companies],
//         mi_idx: RDD[Movie_info_idx],
//         t: RDD[Title]) extends Query {
//  override def execute(): (List[Any]) = {
//
//    val ct_f = ct.filter(_.kind=="production companies").map(_.id -> false)
//    val it_f = it.filter(_.info=="top 250 rank").map(_.id-> false)
//    val mc_f = mc.filter(m => !m.note.contains("as Metro-Goldwyn-Mayer Pictures")
//                              && (m.note.contains("co-production") || m.note.contains("presents")))
//                  .map(m => m.company_type_id -> (m.movie_id, m.note))
//    val mi_idx_s = mi_idx.map(mi => mi.movie_id -> mi.info_type_id)
//    val t_s = t.map(tt => tt.id -> (tt.title, tt.production_year))
//
//    // m.movie_id, m.note
//    val join1 = ct_f.join(mc_f).map(j1 => j1._2._2)
//    //order (t.id=m.movie_id, (t.title, t.production_year), m.note
//    val join2 = t_s.join(join1)
//    //order t.id,(((t.title, t.production_year), m.note),mi.info_type_id) before map
//    val join3 = join2.join(mi_idx_s).map(x => x._2._2 -> (x._2._1))
//
//    val join4 = join3.join(it_f).map(x => x._2._1)
//
//    val res = join4.reduce((a, b) =>
//      ((min_s(a._1._1,b._1._1 ), List(a._1._2, b._1._2).min), min_s(a._2, b._2)))
//
//
//    List(res._1._1, res._1._2, res._2)
//  }
//
//}
//
//class Q2(cn: RDD[Company_name],
//         k: RDD[Keyword],
//         mc: RDD[Movie_companies],
//         mk: RDD[Movie_keyword],
//         t: RDD[Title]) extends Query {
//  override def execute(): (List[Any]) = {
//    val cn_f = cn.filter(_.country_code =="[de]").map(_.id -> false)
//    val k_f = k.filter(_.word == "character-name-in-title").map(_.id -> false)
//    val mc_f = mc.map(x => x.company_id -> x.movie_id)
//    val mk_f = mk.map(x => x.movie_id -> x.word_id)
//    val t_f = t.map(x => x.id -> x.title)
//
//    // mc.movie_id -> 0
//    val join1 = cn_f.join(mc_f).map(x => x._2._2 -> 0)
//    // t.id = mc.movie_id -> t.title
//    val join2 = join1.join(t_f).map(x => x._1 -> x._2._2)
//    // t.id = mc.movie_id = mk.movie_id -> (t.title, mk.word_id)
//    val join3 = join2.join(mk_f)
//    val table_res = join3.join(k_f).map(x => x._2._1._1)
//
//    val res = table_res.reduce(min_s(_,_))
//    List(res)
//  }
//}

//class Q3(k: RDD[Keyword],
//         mi: RDD[Movie_info],
//         mk: RDD[Movie_keyword],
//         t: RDD[Title]) extends Query {
//  override def execute(): (List[Any]) = {
//    val k_f = k.filter(_.word.contains("sequel")).map(_.id -> false)
//    val mi_f = mi.filter(x => List("Sweden", "Norway", "Germany", "Denmark",
//      "Swedish", "Denish", "Norwegian", "German").contains(x.info)).map(_.movie_id -> false)
//    val t_f = t.filter(_.production_year > 2005).map(x => x.id -> x.title)
//    val mk_f = mk.map(x => x.movie_id -> x.word_id)
//
//    //t_id = mi_movie_id -> t.title
//    val join1 = t_f.join(mi_f).map(x => x._1 -> x._2._1)
//    //mk.word -> t.title
//    val join2 = join1.join(mk_f).map(x => x._2._2 -> x._2._1)
//    val table_res = join2.join(k_f).map(_._2._1)
//    val res = table_res.reduce(min_s(_,_))
//
//    List(res)
//  }
//}

//class Q4(it: RDD[Info_type],
//         k: RDD[Keyword],
//         mi_idx: RDD[Movie_info_idx],
//         mk: RDD[Movie_keyword],
//         t: RDD[Title]) extends Query {
//
//  override def execute(): (List[Any]) = {
//    val it_f = it.filter(_.info == "rating").map(_.id -> false)
//    val k_f = k.filter(_.word.contains("sequel")).map(_.id -> false)
//    val mi_idx_f = mi_idx.filter(_.info > "5.0").map(x => x.info_type_id -> (x.movie_id, x.info))
//    val mk_f = mk.map(x => x.word_id -> x.movie_id)
//    val t_f = t.map(x => x.id -> x.title)
//    // mk_movie_id -> false
//    val mk_k_j = k_f.join(mk_f).map(x => x._2._2 -> false)
//    // mi_idx.movie_id -> mi_idx.info
//    val it_idx_j = it_f.join(mi_idx_f).map(x => x._2._2._1 -> x._2._2._2)
//
//    val join1 = mk_k_j.join(it_idx_j).map(x => x._1 -> x._2._2)
//
//    val res_table = t_f.join(join1).map(_._2)
//
//    val res = res_table.reduce((a, b) =>(min_s(a._1,b._1), min_s(a._2, b._2)))
//
//    List(res._1, res._2)
//  }
//}
//
//class Q5(ct: RDD[Company_type],
//         it: RDD[Info_type],
//         mc: RDD[Movie_companies],
//         mi: RDD[Movie_info],
//         t: RDD[Title]) extends Query {
//  override def execute(): (List[Any]) = {
//
//    val mi_f = mi.filter(x => List("Sweden", "Norway", "Germany", "Denmark",
//                              "Swedish", "Denish", "Norwegian", "German").contains(x.info))
//                                .map(x => x.info_type_id -> x.movie_id)
//
//    val t_f = t.filter(_.production_year > 2005).map(x => x.id -> x.title)
//    val mc_f = mc.filter(x => x.note.contains("(theatrical") && x.note.contains("(France)"))
//                  .map(x => x.company_type_id -> x.movie_id)
//    val ct_f = ct.filter(_.kind == "production companies").map(_.id -> false)
//    val it_f = it.map(_.id -> false)
//
//    //mi_movie_id -> false
//    val mi_it_j = mi_f.join(it_f).map(_._2._1 -> false)
//    //mc.movie_id -> false
//    val mc_ct_j = mc_f.join(ct_f).map(_._2._1 -> false)
//    //mi_movie_id  = mc.movie_id -> false
//    val join1 = mi_it_j.join(mc_ct_j).map(_._1 -> false)
//
//    val res_table = t_f.join(join1).map(_._2._1)
//    val res = res_table.reduce(min_s(_,_))
//
//    List(res)
//  }
//}