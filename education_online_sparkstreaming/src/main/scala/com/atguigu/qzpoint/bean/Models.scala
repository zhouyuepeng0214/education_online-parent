package com.atguigu.qzpoint.bean

case class Register(
                   userId : Int,
                   platId : String,
                   creatTime : String
                 ) {

}

case class LearnModel(
                       userId: Int,
                       cwareId: Int,
                       videoId: Int,
                       chapterId: Int,
                       edutypeId: Int,
                       subjectId: Int,
                       sourceType: String,
                       speed: Int,
                       ts: Long,
                       te: Long,
                       ps: Int,
                       pe: Int
                     )
