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
    // NOTE: sqlInsert does not check schemas
    // insert
    e := Employees()
    c := [
     CCol("a", Str#, [:]),
     CCol("b", Str#, [:]),
     CCol("c", Str#, [:]),
    ]
    Method m := Slot.find("carbonite::CUtil.sqlInsert")
    verifyEq(m.call(e, c), """insert into employees ("a","b","c") values(?,?,?)""")

    // delete
    w := ["foo", "bar"]
    m = Slot.find("carbonite::CUtil.sqlDelete")
    verifyEq(m.call(e, w), """delete from employees where "foo" = ? and "bar" = ?""")
  }
}