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

    // update
    m = Slot.find("carbonite::CUtil.sqlUpdate")
    verifyEq(m.call(e, c), """update employees set "a"=?,"b"=?,"c"=? where id = ?""")

    // delete
    w := ["foo", "bar"]
    m = Slot.find("carbonite::CUtil.sqlDelete")
    verifyEq(m.call(e, w), """delete from employees where "foo" = ? and "bar" = ?""")
  }

  Void testBatch()
  {
    // uneven
    a := [1,2,3,4,5,6,7,8,9,10]
    x := [,]
    doBatch(a, 3) |c| { x.add(c) }
    verifyEq(x.size, 4)
    verifyEq(x[0], [1,2,3])
    verifyEq(x[1], [4,5,6])
    verifyEq(x[2], [7,8,9])
    verifyEq(x[3], [10])

    // even
    a = [1,2,3,4,5,6,7,8,9]
    x = [,]
    doBatch(a, 3) |c| { x.add(c) }
    verifyEq(x.size, 3)
    verifyEq(x[0], [1,2,3])
    verifyEq(x[1], [4,5,6])
    verifyEq(x[2], [7,8,9])

    // smaller
    a = [1,2,3]
    x = [,]
    doBatch(a, 100) |c| { x.add(c) }
    verifyEq(x.size, 1)
    verifyEq(x[0], [1,2,3])
  }

  private Void doBatch(Obj[] list, Int size, |Obj[] chunk| f)
  {
    Method m := Slot.find("carbonite::CUtil.batch")
    m.call(list, size, f)
  }
}