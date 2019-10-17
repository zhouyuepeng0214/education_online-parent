package com.atguigu.education_online.qz.dao

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

object AdsQzDao {

  //各试卷平均耗时、平均分
  def getAvgSPendTimeAndScore(sparkSession: SparkSession, dt: String) : sql.DataFrame = {
    sparkSession.sql(s"select paperviewid,paperviewname,cast(avg(score) as decimal(4,1)) as avgscore," +
      s"cast(avg(spendtime) as decimal(10,2)) avgspendtime,dt,dn from dws.dws_user_paper_detail where dt='${dt}' " +
      s"group by paperviewid,paperviewname,dt,dn order by avgscore desc ,avgspendtime")
  }

  //各试卷最高分、最低分
  def getTopScore(sparkSession: SparkSession, dt: String) : sql.DataFrame = {
    sparkSession.sql(s"select paperviewid,paperviewname,cast(max(score) as decimal(4,1)) as maxscore," +
      s"cast(min(score) as decimal(4,1)) minscore,dt,dn from dws.dws_user_paper_detail where dt='${dt}' " +
      s"group by paperviewid,paperviewname,dt,dn ")
  }
  //按试卷分组统计每份试卷的分数前三用户详情 todo 分数前三并列
  def getTop3UserDetail(sparkSession: SparkSession, dt: String) : sql.DataFrame = {
    sparkSession.sql(s"select * from(select userid,paperviewname,chaptername,pointname,sitecoursename,coursename,majorname," +
      s"shortname,sitename,papername,score, dense_rank() over(partition by paperviewid order by score desc) as rk,dt," +
      s"dn from dws.dws_user_paper_detail) t where rk <= 3")
  }
  //按试卷分组统计每份试卷的分数倒数前三的用户详情
  def getLow3UserDetail(sparkSession: SparkSession, dt: String) : sql.DataFrame = {
    sparkSession.sql("select *from (select userid,paperviewname,chaptername,pointname,sitecoursename,coursename," +
      "majorname,shortname,sitename,papername,score,dense_rank() over (partition by paperviewid order by score asc) as rk,dt," +
      "dn from dws.dws_user_paper_detail) t where rk<4")
  }

  //统计各试卷各分段的用户id，分段有0-20,20-40,40-60，60-80,80-100
  def getPaperScoreSegmentUser(sparkSession: SparkSession, dt: String) : sql.DataFrame = {
    sparkSession.sql(s"select paperviewid,paperviewname,score_segment,concat_ws(',',collect_list(cast(userid as string))) as userids,dt,dn " +
      s"from (select paperviewid,paperviewname,userid," +
      s"case when score >=0 and score <=20 then '0-20' " +
      s"when score >20 and score <=40 then '20-40' " +
      s"when score >40 and score <=60 then '40-60' " +
      s"when score >60 and score <=80 then '60-80' " +
      s"when score >80 and score <=100 then '80-100' " +
      s"end as score_segment," +
      s"dt,dn from dws.dws_user_paper_detail where dt='${dt}') t group by paperviewid,paperviewname,score_segment,dt,dn")
  }

  //统计试卷未及格的人数，及格的人数，试卷的及格率 ;及格分数60
  def getPaperPassDetail(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql("select paperviewid,paperviewname,unpasscount,passcount,cast(t.passcount/(t.passcount+t.unpasscount) as decimal(4,2)) as rate,dt,dn" +
      "   from(select a.paperviewid,a.paperviewname,a.unpasscount,a.dt,a.dn,b.passcount from " +
      s"(select paperviewid,paperviewname,count(*) unpasscount,dt,dn from dws.dws_user_paper_detail where dt='${dt}' and score between 0 and 60 group by" +
      s" paperviewid,paperviewname,dt,dn) a join (select paperviewid,count(*) passcount,dn from  dws.dws_user_paper_detail  where dt='${dt}' and score >60  " +
      "group by paperviewid,dn) b on a.paperviewid=b.paperviewid and a.dn=b.dn)t")

  }
  //统计各题的错误数，正确数，错题率
  def getQuestionDetail(sparkSession: SparkSession, dt: String) : sql.DataFrame = {
    sparkSession.sql(s"select a.questionid,a.errcount,b.rightcount,cast(a.errcount/(a.errcount+b.rightcount) " +
      s"as decimal(4,2)) as rate,a.dt,a.dn from (select questionid,count(*) errcount,dt,dn from " +
      s"dws.dws_user_paper_detail where dt='${dt}' and user_question_answer='0' group by questionid,dt,dn) a join " +
      s"(select questionid,count(*) rightcount,dt,dn from dws.dws_user_paper_detail where dt='${dt}' and user_question_answer='1' " +
      s"group by questionid,dt,dn) b on a.questionid=b.questionid and a.dn=b.dn order by errcount desc")
  }




}
