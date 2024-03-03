//
// Copyright (c) 2024, Novant LLC
// All Rights Reserved
//
// History:
//   3 Mar 2024  Andy Frank  Creation
//

using carbonite

*************************************************************************
** BasicTest
*************************************************************************

class CUtilTest : Test
{
  Void testSql()
  {
    e := Employees()
    Method m := Slot.find("carbonite::CUtil.sqlInsert")
    verifyEq(m.call(e, ["a","b","c"]), "insert into employees (a,b,c) values(?,?,?)")
  }
}