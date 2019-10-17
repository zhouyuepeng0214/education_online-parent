package com.atguigu.education_online.member.bean

case class DwdMemberRegtype(
                             uid: Int,
                             appkey: String,
                             appregurl: String,
                             bdp_uuid: String,
                             createtime: String,
                             domain: String,
                             isranreg: String,
                             regsource: String,
                             var regsourcename: String,
                             websiteid: Int,
                             dt: String,
                             dn: String) {

}

case class DwdBaseadlog(
                         adid: Int,
                         adname: String,
                         dn: String
                       ) {

}

case class DwdBaswewebsite(
                            siteid: Int,
                            sitename: String,
                            siteurl: String,
                            `delete`: Int,
                            createtime: String,
                            creator: String,
                            dn: String
                          ) {

}

case class DwdMember(
                      uid: Int,
                      ad_id: Int,
                      birthday: String,
                      email: String,
                      var fullname: String,
                      iconurl: String,
                      lastlogin: String,
                      mailaddr: String,
                      memberlevel: String,
                      var password: String,
                      paymoney: String,
                      var phone: String,
                      qq: String,
                      register: String,
                      regupdatetime: String,
                      unitname: String,
                      userip: String,
                      zipcode: String,
                      dt: String,
                      dn: String
                    ) {

}

case class DwdPcentermempaymoney(
                                  uid: Int,
                                  paymoney: String,
                                  siteid: Int,
                                  vip_id: Int,
                                  dt: String,
                                  dn: String
                                ) {

}

case class DwdPcenterMemViplevel(
                                  vip_id: Int,
                                  vip_level: String,
                                  start_time: String,
                                  end_time: String,
                                  last_modify_time: String,
                                  max_free: String,
                                  min_free: String,
                                  next_level: String,
                                  operator: String,
                                  dn: String
                                ) {

}

case class DwsMember(
                      uid: Int,
                      ad_id: Int,
                      fullname: String,
                      iconurl: String,
                      lastlogin: String,
                      mailaddr: String,
                      memberlevel: String,
                      password: String,
                      paymoney: String,
                      phone: String,
                      qq: String,
                      register: String,
                      regupdatetime: String,
                      unitname: String,
                      userip: String,
                      zipcode: String,
                      appkey: String,
                      appregurl: String,
                      bdp_uuid: String,
                      reg_createtime: String,
                      domain: String,
                      isranreg: String,
                      regsource: String,
                      regsourcename: String,
                      adname: String,
                      siteid: String,
                      sitename: String,
                      siteurl: String,
                      site_delete: String,
                      site_createtime: String,
                      site_creator: String,
                      vip_id: String,
                      vip_level: String,
                      vip_start_time: String,
                      vip_end_time: String,
                      vip_last_modify_time: String,
                      vip_max_free: String,
                      vip_min_free: String,
                      vip_next_level: String,
                      vip_operator: String,
                      dt: String,
                      dn: String) {

}

case class MemberZipper(
                         uid: Int,
                         var paymoney: String,
                         vip_level: String,
                         start_time: String,
                         var end_time: String,
                         dn: String
                       ) {

}

case class MemberZipperResult(list: List[MemberZipper]) {

}


case class QueryResult(
                        uid: Int,
                        ad_id: Int,
                        memberlevel: String,
                        register: String,
                        appregurl: String, //注册来源url
                        regsource: String,
                        regsourcename: String,
                        adname: String,
                        siteid: String,
                        sitename: String,
                        vip_level: String,
                        paymoney: BigDecimal,
                        dt: String,
                        dn: String
                      )

