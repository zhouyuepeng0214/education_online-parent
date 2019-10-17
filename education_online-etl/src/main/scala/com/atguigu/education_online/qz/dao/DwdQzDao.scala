package com.atguigu.education_online.qz.dao

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

object DwdQzDao {

  def getDwdQzChapter(sparkSession: SparkSession,dt : String): sql.DataFrame = {
    sparkSession.sql(s"select chapterid,chapterlistid,chaptername,sequence,showstatus,status," +
      s"creator as chapter_creator,createtime as chapter_createtime,courseid as chapter_courseid,chapternum," +
      s"outchapterid,dt,dn from dwd.dwd_qz_chapter where dt='${dt}'")
  }

  def getDwdQzChapterList(sparkSession: SparkSession,dt : String): sql.DataFrame = {
    sparkSession.sql("select chapterlistid,chapterlistname,chapterallnum,dn from " +
      s"dwd.dwd_qz_chapter_list where dt='${dt}'")
  }

  def getDwdQzPoint(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql("select pointid,pointname,pointyear,chapter,excisenum,pointlistid,chapterid," +
      "pointdescribe,pointlevel,typelist,score as point_score,thought,remid,pointnamelist,typelistids,pointlist,dn from " +
      s"dwd.dwd_qz_point where dt='${dt}'")
  }


  def getDwdQzPointQuestion(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql(s"select pointid,questionid,questype,dn from dwd.dwd_qz_point_question where dt='${dt}'")
  }

  def getDwdQzSiteCourse(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql("select sitecourseid,siteid,courseid,sitecoursename,coursechapter,sequence,status," +
      "creator as sitecourse_creator,createtime as sitecourse_createtime,helppaperstatus,servertype,boardid,showstatus,dt,dn " +
      s"from dwd.dwd_qz_site_course where dt='${dt}'")
  }

  def getDwdQzCourse(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql("select courseid,majorid,coursename,isadvc,chapterlistid,pointlistid,dn from " +
      s"dwd.dwd_qz_course where dt='${dt}'")
  }

  def getDwdQzCourseEduSubject(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql("select courseeduid,edusubjectid,courseid,dn from dwd.dwd_qz_course_edusubject " +
      s"where dt='${dt}'")
  }

  def getDwdQzMajor(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql("select majorid,businessid,siteid,majorname,shortname,status,sequence,creator as major_creator," +
      s"createtime as major_createtime,dt,dn from dwd.dwd_qz_major where dt='${dt}'")
  }

  def getDwdQzWebsite(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql("select siteid,sitename,domain,multicastserver,templateserver,creator," +
      s"createtime,multicastgateway,multicastport,dn from dwd.dwd_qz_website where dt='${dt}'")
  }

  def getDwdQzBusiness(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql(s"select businessid,businessname,dn from dwd.dwd_qz_business where dt='${dt}'")
  }

  def getDwdQzPaperView(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql("select paperviewid,paperid,paperviewname,paperparam,openstatus,explainurl,iscontest," +
      "contesttime,conteststarttime,contestendtime,contesttimelimit,dayiid,status,creator as paper_view_creator," +
      "createtime as paper_view_createtime,paperviewcatid,modifystatus,description,papertype,downurl,paperuse," +
      s"paperdifficult,testreport,paperuseshow,dt,dn from dwd.dwd_qz_paper_view where dt='${dt}'")
  }

  def getDwdQzCenterPaper(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql(s"select paperviewid,sequence,centerid,dn from dwd.dwd_qz_center_paper where dt='${dt}'")
  }

  def getDwdQzPaper(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql("select paperid,papercatid,courseid,paperyear,chapter,suitnum,papername,totalscore,chapterid," +
      s"chapterlistid,dn from dwd.dwd_qz_paper where dt='${dt}'")
  }

  def getDwdQzCenter(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql("select centerid,centername,centeryear,centertype,centerparam,provideuser," +
      s"centerviewtype,stage,dn from dwd.dwd_qz_center where dt='${dt}'")
  }

  def getDwdQzQuestion(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql("select questionid,parentid,questypeid,quesviewtype,content,answer,analysis,limitminute," +
      "score,splitscore,status,optnum,lecture,creator,createtime,modifystatus,attanswer,questag,vanalysisaddr,difficulty," +
      s"quesskill,vdeoaddr,dt,dn from  dwd.dwd_qz_question where dt='${dt}'")
  }

  def getDwdQzQuestionType(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql("select questypeid,viewtypename,description,papertypename,remark,splitscoretype,dn from " +
      s"dwd.dwd_qz_question_type where dt='${dt}'")
  }

  def getDwdQzMemberPaperQuestion(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql("select userid,paperviewid,chapterid,sitecourseid,questionid,majorid,useranswer,istrue,lasttime,opertype," +
      s"paperid,spendtime,score,question_answer,dt,dn from dwd.dwd_qz_member_paper_question where dt='${dt}'")
  }





}
