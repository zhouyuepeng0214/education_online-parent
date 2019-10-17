package com.atguigu.education_online.qz.service

import com.atguigu.education_online.qz.dao.AdsQzDao
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object AdsQzService {

  def getTarget(sparkSession: SparkSession, dt: String) = {

    val avgDetail = AdsQzDao.getAvgSPendTimeAndScore(sparkSession, dt)
    avgDetail.limit(5).show()

    val topscore = AdsQzDao.getTopScore(sparkSession, dt)
    topscore.limit(5).show()

    val top3UserDetail = AdsQzDao.getTop3UserDetail(sparkSession, dt)
    top3UserDetail.limit(100).show()

    val low3UserDetail = AdsQzDao.getLow3UserDetail(sparkSession, dt)
    low3UserDetail.limit(20).show()

    val paperScore = AdsQzDao.getPaperScoreSegmentUser(sparkSession, dt)
    paperScore.limit(20).show()

    val paperPassDetail = AdsQzDao.getPaperPassDetail(sparkSession, dt)
    paperPassDetail.limit(20).show()

    val questionDetail = AdsQzDao.getQuestionDetail(sparkSession, dt)
    questionDetail.limit(20).show()
  }

  def getTargetApi(sparkSession: SparkSession, dt: String) = {
    import org.apache.spark.sql.functions._
    // todo 1.各试卷平均耗时、平均分
        sparkSession.sql("select paperviewid,paperviewname,score,spendtime,dt,dn from dws.dws_user_paper_detail")
          .where(s"dt='${dt}'")
          .groupBy("paperviewid", "paperviewname", "dt", "dn")
          .agg(avg("score").cast("decimal(4,1)").as("avgscore"),
            avg("spendtime").cast("decimal(10,1)").as("avgspendtime"))
          .select("paperviewid", "paperviewname", "avgscore", "avgspendtime", "dt", "dn")
          .coalesce(1).write.mode(SaveMode.Append).insertInto("ads.ads_paper_avgtimeandscore")

        sparkSession.sql("select * from ads.ads_paper_avgtimeandscore limit 5").show()

    // todo 2.各试卷最高分、最低分
        sparkSession.sql("select paperviewid,paperviewname,score,dt,dn from dws.dws_user_paper_detail")
          .where(s"dt='${dt}'")
          .groupBy("paperviewid", "paperviewname", "dt", "dn")
          .agg(max("score").as("maxscore"),min("score").as("minscore"))
          .select("paperviewid", "paperviewname", "maxscore", "minscore", "dt", "dn")
          .coalesce(1)
          .write.mode(SaveMode.Append)
          .insertInto("ads.ads_paper_maxdetail")

        sparkSession.sql("select * from ads.ads_paper_maxdetail limit 5").show()

    // todo 3.按试卷分组统计每份试卷的分数前三用户详情 todo 分数前三并列
        sparkSession.sql("select *from dws.dws_user_paper_detail")
          .where(s"dt='${dt}'")
          .select("userid", "paperviewid", "paperviewname", "chaptername", "pointname"
            , "sitecoursename", "coursename", "majorname", "shortname", "papername", "score", "dt", "dn")
          .withColumn("rk", dense_rank().over(Window.partitionBy("paperviewid").orderBy(desc("score"))))
          .where("rk<=3")
          .select("userid", "paperviewid", "paperviewname", "chaptername", "pointname", "sitecoursename"
            , "coursename", "majorname", "shortname", "papername", "score", "rk", "dt", "dn")
          .coalesce(1)
          .write.mode(SaveMode.Append)
          .insertInto("ads.ads_top3_userdetail")

        sparkSession.sql("select * from ads.ads_top3_userdetail limit 5").show()

    // todo 4.按试卷分组统计每份试卷的分数倒数前三的用户详情
    sparkSession.sql("select *from dws.dws_user_paper_detail")
      .where(s"dt='${dt}'").select("userid", "paperviewid", "paperviewname", "chaptername", "pointname"
      , "sitecoursename", "coursename", "majorname", "shortname", "papername", "score", "dt", "dn")
      .withColumn("rk", dense_rank().over(Window.partitionBy("paperviewid").orderBy("score")))
      .where("rk<4")
      .select("userid", "paperviewid", "paperviewname", "chaptername", "pointname", "sitecoursename"
        , "coursename", "majorname", "shortname", "papername", "score", "rk", "dt", "dn")
      .coalesce(1).write.mode(SaveMode.Append).insertInto("ads.ads_low3_userdetail")

    sparkSession.sql("select * from ads.ads_low3_userdetail limit 5").show()

    // todo 5.统计各试卷各分段的用户id，分段有0-20,20-40,40-60，60-80,80-100
    sparkSession.sql("select *from dws.dws_user_paper_detail")
      .where(s"dt='${dt}'")
      .select("paperviewid", "paperviewname", "userid", "score", "dt", "dn")
      .withColumn("score_segment",
        when(col("score").between(0, 20), "0-20")
          .when(col("score") >20 && col("score") <= 40,"20-40")
          .when(col("score") > 40 && col("score") <= 60, "40-60")
          .when(col("score") > 60 && col("score") <= 80, "60-80")
          .when(col("score") > 80 && col("score") <= 100, "80-100"))
      .drop("score")
      .groupBy("paperviewid", "paperviewname", "score_segment", "dt", "dn")
      .agg(concat_ws(",",collect_list(col("userid").cast("String").as("userids"))).as("userids"))
      .select("paperviewid", "paperviewname", "score_segment", "userids", "dt", "dn")
      .orderBy("paperviewid","score_segment")
      .coalesce(1)
      .write.mode(SaveMode.Append)
      .insertInto("ads.ads_paper_scoresegment_user")

    sparkSession.sql("select * from ads.ads_paper_scoresegment_user limit 5").show()

    // todo 6.统计试卷未及格的人数，及格的人数，试卷的及格率 ;及格分数60
    val result: DataFrame = sparkSession.sql("select *from dws.dws_user_paper_detail").cache()
    val unpasscount: DataFrame = result.select("paperviewid", "paperviewname","score","dn", "dt")
      .where(s"dt='${dt}'").where("score between 0 and 60")
      .groupBy("paperviewid", "paperviewname", "dn", "dt")
      .agg(count("score").as("unpasscount")).drop("score")
    val passcount: DataFrame = result.select("paperviewid", "score","dn")
      .where(s"dt='${dt}'").where("score > 60 and score <= 100")
      .groupBy("paperviewid", "dn")
      .agg(count("score").as("passcount")).drop("score")

    unpasscount.join(passcount,Seq("paperviewid","dn"))
      .withColumn("rate",col("passcount")./(col("passcount") + col("unpasscount"))
        .cast("decimal(4,2)"))
      .select("paperviewid","paperviewname","passcount","unpasscount","rate","dt","dn")
      .coalesce(1)
      .write.mode(SaveMode.Append)
      .insertInto("ads.ads_user_paper_detail")

    sparkSession.sql("select * from ads.ads_user_paper_detail limit 5").show()

    // todo 7.统计各题的错误数，正确数，错题率
    val errCount: DataFrame = result.select("questionid", "user_question_answer","dt", "dn")
      .where(s"dt='${dt}'").where("user_question_answer = '0'")
      .groupBy("questionid", "dt", "dn")
      .agg(count("user_question_answer").as("errcount"))

    val rightCount: DataFrame = result.select("questionid", "user_question_answer","dn")
      .where(s"dt='${dt}'").where("user_question_answer = '1'")
      .groupBy("questionid","dn")
      .agg(count("user_question_answer").as("rightcount"))

    errCount.join(rightCount,Seq("questionid","dn"))
        .withColumn("rate",col("errcount")./(col("errcount") + col("rightcount"))
          .cast("decimal(4,2)"))
        .select("questionid","errcount","rightcount","rate","dt","dn")
        .coalesce(1)
        .write.mode(SaveMode.Append)
      .insertInto("ads.ads_user_question_detail")

        sparkSession.sql("select * from ads.ads_user_question_detail limit 5").orderBy(desc("rate")).show()

    // todo 此算子在程序最后的话就不需要了，程序执行完毕就释放内存了
    result.unpersist()

  }

}
