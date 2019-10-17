package com.atguigu.education_online.qz.service

import com.atguigu.education_online.qz.dao.{DwdQzDao, DwsQzDao}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object DwsQzService {

  def saveDwsQzChapter(sparkSession: SparkSession, dt: String) = {
    val dwdQzChapter: DataFrame = DwdQzDao.getDwdQzChapter(sparkSession,dt)
    val dwdQzChapterList: DataFrame = DwdQzDao.getDwdQzChapterList(sparkSession,dt)
    val dwdQzPoint: DataFrame = DwdQzDao.getDwdQzPoint(sparkSession,dt)
    val dwdQzPointQuestion: DataFrame = DwdQzDao.getDwdQzPointQuestion(sparkSession,dt)

    dwdQzChapter.join(dwdQzChapterList,Seq("chapterlistid","dn"))
      .join(dwdQzPoint,Seq("chapterid","dn"))
      .join(dwdQzPointQuestion,Seq("pointid","dn"))
      .select("chapterid", "chapterlistid", "chaptername", "sequence", "showstatus", "status",
        "chapter_creator", "chapter_createtime", "chapter_courseid", "chapternum", "chapterallnum", "outchapterid", "chapterlistname",
        "pointid", "questionid", "questype", "pointname", "pointyear", "chapter", "excisenum", "pointlistid", "pointdescribe",
        "pointlevel", "typelist", "point_score", "thought", "remid", "pointnamelist", "typelistids", "pointlist", "dt", "dn")
      .coalesce(1).write.mode(SaveMode.Append).insertInto("dws.dws_qz_chapter")

    sparkSession.sql("select * from dws.dws_qz_chapter limit 3").show()

  }

  def saveDwsQzCourse(sparkSession: SparkSession, dt: String) : Unit= {
    val dwdQzCourse: DataFrame = DwdQzDao.getDwdQzCourse(sparkSession,dt)
    val dwdQzSiteCourse: DataFrame = DwdQzDao.getDwdQzSiteCourse(sparkSession,dt)
    val dwdQzCourseEduSubject: DataFrame = DwdQzDao.getDwdQzCourseEduSubject(sparkSession,dt)

    dwdQzSiteCourse.join(dwdQzCourse, Seq("courseid", "dn"))
      .join(dwdQzCourseEduSubject, Seq("courseid", "dn"))
      .select("sitecourseid", "siteid", "courseid", "sitecoursename", "coursechapter",
        "sequence", "status", "sitecourse_creator", "sitecourse_createtime", "helppaperstatus", "servertype", "boardid",
        "showstatus", "majorid", "coursename", "isadvc", "chapterlistid", "pointlistid", "courseeduid", "edusubjectid"
        , "dt", "dn")
    .coalesce(1).write.mode(SaveMode.Append).insertInto("dws.dws_qz_course")
    sparkSession.sql("select * from dws.dws_qz_course limit 3").show()

  }

  def saveDwsQzMajor(sparkSession: SparkSession, dt: String) = {
    val dwdQzMajor: DataFrame = DwdQzDao.getDwdQzMajor(sparkSession, dt)
    val dwdQzWebsite: DataFrame = DwdQzDao.getDwdQzWebsite(sparkSession, dt)
    val dwdQzBusiness: DataFrame = DwdQzDao.getDwdQzBusiness(sparkSession, dt)
    dwdQzMajor.join(dwdQzWebsite, Seq("siteid", "dn"))
      .join(dwdQzBusiness, Seq("businessid", "dn"))
      .select("majorid", "businessid", "siteid", "majorname", "shortname", "status", "sequence",
        "major_creator", "major_createtime", "businessname", "sitename", "domain", "multicastserver", "templateserver",
        "multicastgateway", "multicastport", "dt", "dn")
    .coalesce(1).write.mode(SaveMode.Append).insertInto("dws.dws_qz_major")

    sparkSession.sql("select * from dws.dws_qz_major limit 3").show()
  }

  def saveDwsQzPaper(sparkSession: SparkSession, dt: String) = {
    val dwdQzPaperView: DataFrame = DwdQzDao.getDwdQzPaperView(sparkSession, dt)
    val dwdQzCenterPaper: DataFrame = DwdQzDao.getDwdQzCenterPaper(sparkSession, dt)
    val dwdQzCenter: DataFrame = DwdQzDao.getDwdQzCenter(sparkSession, dt)
    val dwdQzPaper: DataFrame = DwdQzDao.getDwdQzPaper(sparkSession, dt)
    dwdQzPaperView.join(dwdQzCenterPaper, Seq("paperviewid", "dn"), "left")
      .join(dwdQzCenter, Seq("centerid", "dn"), "left")
      .join(dwdQzPaper, Seq("paperid", "dn"))
      .select("paperviewid", "paperid", "paperviewname", "paperparam", "openstatus", "explainurl", "iscontest"
        , "contesttime", "conteststarttime", "contestendtime", "contesttimelimit", "dayiid", "status", "paper_view_creator",
        "paper_view_createtime", "paperviewcatid", "modifystatus", "description", "paperuse", "paperdifficult", "testreport",
        "paperuseshow", "centerid", "sequence", "centername", "centeryear", "centertype", "provideuser", "centerviewtype",
        "stage", "papercatid", "courseid", "paperyear", "suitnum", "papername", "totalscore", "chapterid", "chapterlistid",
        "dt", "dn")
    .coalesce(1).write.mode(SaveMode.Append).insertInto("dws.dws_qz_paper")
    sparkSession.sql("select * from dws.dws_qz_paper limit 3").show()
  }

  def saveDwsQzQuestionTpe(sparkSession: SparkSession, dt: String) = {
    val dwdQzQuestion : DataFrame = DwdQzDao.getDwdQzQuestion(sparkSession, dt)
    val dwdQzQuestionType : DataFrame = DwdQzDao.getDwdQzQuestionType(sparkSession, dt)
    dwdQzQuestion.join(dwdQzQuestionType, Seq("questypeid", "dn"))
      .select("questionid", "parentid", "questypeid", "quesviewtype", "content", "answer", "analysis"
        , "limitminute", "score", "splitscore", "status", "optnum", "lecture", "creator", "createtime", "modifystatus"
        , "attanswer", "questag", "vanalysisaddr", "difficulty", "quesskill", "vdeoaddr", "viewtypename", "papertypename",
        "remark", "splitscoretype", "dt", "dn")
    .coalesce(1).write.mode(SaveMode.Append).insertInto("dws.dws_qz_question")

    sparkSession.sql("select * from dws.dws_qz_question limit 3").show()
  }

  def saveDwsUserPaperDetail(sparkSession: SparkSession, dt: String): Unit = {
    val dwdQzMemberPaperQuestion: DataFrame = DwdQzDao.getDwdQzMemberPaperQuestion(sparkSession,dt)
      .withColumnRenamed("question_answer", "user_question_answer")
      .drop("paperid")
    val dwsQzChapter: DataFrame = DwsQzDao.getDwsQzChapter(sparkSession,dt).drop("courseid")
    val dwsQzMajor: DataFrame = DwsQzDao.getDwsQzMajor(sparkSession,dt)
    val dwsQzPaper: DataFrame = DwsQzDao.getDwsQzPaper(sparkSession,dt).drop("courseid").drop("chapterlistid")
    val dwsQzQuestion: DataFrame = DwsQzDao.getDwsQzQuestion(sparkSession,dt)
    val dwsQzCourse: DataFrame = DwsQzDao.getDwsQzCourse(sparkSession,dt).withColumnRenamed("sitecourse_creator", "course_creator")
      .withColumnRenamed("sitecourse_createtime", "course_createtime")
      .drop("majorid").drop("chapterlistid").drop("pointlistid")

    dwdQzMemberPaperQuestion.join(dwsQzCourse,Seq("sitecourseid","dn"))
      .join(dwsQzChapter,Seq("chapterid", "dn"))
      .join(dwsQzMajor, Seq("majorid", "dn"))
      .join(dwsQzPaper, Seq("paperviewid", "dn"))
      .join(dwsQzQuestion, Seq("questionid", "dn"))
      .select("userid", "courseid", "questionid", "useranswer", "istrue", "lasttime", "opertype",
        "paperid", "spendtime", "chapterid", "chaptername", "chapternum",
        "chapterallnum", "outchapterid", "chapterlistname", "pointid", "questype", "pointyear", "chapter", "pointname"
        , "excisenum", "pointdescribe", "pointlevel", "typelist", "point_score", "thought", "remid", "pointnamelist",
        "typelistids", "pointlist", "sitecourseid", "siteid", "sitecoursename", "coursechapter", "course_sequence", "course_status"
        , "course_creator", "course_createtime", "servertype", "helppaperstatus", "boardid", "showstatus", "majorid", "coursename",
        "isadvc", "chapterlistid", "pointlistid", "courseeduid", "edusubjectid", "businessid", "majorname", "shortname",
        "major_status", "major_sequence", "major_creator", "major_createtime", "businessname", "sitename",
        "domain", "multicastserver", "templateserver", "multicastgateway", "multicastport", "paperviewid", "paperviewname", "paperparam",
        "openstatus", "explainurl", "iscontest", "contesttime", "conteststarttime", "contestendtime", "contesttimelimit",
        "dayiid", "paper_status", "paper_view_creator", "paper_view_createtime", "paperviewcatid", "modifystatus", "description", "paperuse",
        "testreport", "centerid", "paper_sequence", "centername", "centeryear", "centertype", "provideuser", "centerviewtype",
        "paper_stage", "papercatid", "paperyear", "suitnum", "papername", "totalscore", "question_parentid", "questypeid",
        "quesviewtype", "question_content", "question_answer", "question_analysis", "question_limitminute", "score",
        "splitscore", "lecture", "question_creator", "question_createtime", "question_modifystatus", "question_attanswer",
        "question_questag", "question_vanalysisaddr", "question_difficulty", "quesskill", "vdeoaddr", "question_description",
        "question_splitscoretype", "user_question_answer", "dt", "dn")
      .coalesce(1)
      .write.mode(SaveMode.Append)
      .insertInto("dws.dws_user_paper_detail")

    sparkSession.sql("select * from dws.dws_user_paper_detail limit 3").show()

  }


}
