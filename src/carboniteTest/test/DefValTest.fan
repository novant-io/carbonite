//
// Copyright (c) 2022, Novant LLC
// All Rights Reserved
//
// History:
//   5 Dec 2022  Andy Frank  Creation
//

using carbonite

*************************************************************************
** DefValTests
*************************************************************************

const class DefValTests1 : CTable
{
  override const Str name := "def_val_tests"
  override const CCol[] cols := [
    CCol("id",     Int#,   [:]),
    CCol("name",   Str#,   ["def_val":"Bob"]),
    CCol("age",    Int#,   ["def_val":40]),
    CCol("picks1", Int[]#, ["def_val":[,]]),
    // TODO: need to fix SqliteStoreImpl.describe impl
    // CCol("picks2", Int[]#, ["def_val":[5,7,9]]),
    // TODO
    // CCol("date",    Date#, ["def_val":Date("2022-12-05")]),
  ]
}

const class DefValTests2 : CTable
{
  override const Str name := "def_val_tests"
  override const CCol[] cols := [
    CCol("id",     Int#,   [:]),
    CCol("name",   Str#,   ["def_val":"Bob"]),
    CCol("age",    Int#,   ["def_val":40]),
    CCol("picks1", Int[]#, ["def_val":[,]]),
    // TODO: need to fix SqliteStoreImpl.describe impl
    // CCol("picks2", Int[]#, ["def_val":[5,7,9]]),
    // TODO
    // CCol("date",    Date#, ["def_val":Date("2022-12-05")]),
    CCol("new_col", Int#,  ["def_val":7])
  ]
}

const class DefValTestsErr1 : CTable
{
  override const Str name := "def_val_tests"
  override const CCol[] cols := [
    CCol("id",      Int#,  [:]),
    CCol("name",    Str#,  ["def_val":"Bob"]),
    CCol("age",     Int#,  ["def_val":40]),
    // TODO
    // CCol("date",    Date#, ["def_val":Date("2022-12-05")]),
    CCol("err_col", Int#,  [:])
  ]
}

const class DefValTestsErr2 : CTable
{
  override const Str name := "def_val_tests"
  override const CCol[] cols := [
    CCol("id",      Int#,  [:]),
    CCol("name",    Str#,  ["def_val":"Bob"]),
    CCol("age",     Int#,  ["def_val":40]),
    // TODO
    // CCol("date",    Date#, ["def_val":Date("2022-12-05")]),
    CCol("err_col", Int#,  ["def_val":"should be int"])
  ]
}

*************************************************************************
** DefValTest
*************************************************************************

class DefValTest : AbstractStoreTest
{
  Void test()
  {
    eachImpl([DefValTests1#]) |s|
    {
      // add row with all defs
      CTable d := s.table(DefValTests1#)
      d.create(["id":1])
      verifyEq(d.get(1)->id,     1)
      verifyEq(d.get(1)->name,   "Bob")
      verifyEq(d.get(1)->age,    40)
      verifyEq(d.get(1)->picks1, Int[,])
      // verifyEq(d.get(1)->date, Date("2022-12-05"))

      // add row with explicit vals for all cols
      d.create(["id":2, "name":"Billy", "age":23]) //, "date":Date("1985-12-20")])
      verifyEq(d.get(2)->id,     2)
      verifyEq(d.get(2)->name,   "Billy")
      verifyEq(d.get(2)->age,    23)
      verifyEq(d.get(1)->picks1, Int[,])
      // verifyEq(d.get(2)->date, Date("1985-12-20"))
    }

    // test adding a non-null with with a def_val
    eachImpl([DefValTests2#]) |s|
    {
      // add row with all defs
      CTable d := s.table(DefValTests2#)
      d.create(["id":3])
      verifyEq(d.get(3)->id,   3)
      verifyEq(d.get(3)->name, "Bob")
      verifyEq(d.get(3)->age,  40)
      // verifyEq(d.get(3)->date, Date("2022-12-05"))
      verifyEq(d.get(3)->new_col, 7)
    }

    // test adding a non-null colum without a def_val
    verifySqlErr {
      this.eachImpl([DefValTestsErr1#]) |s| { /**/ }
    }

    // test def_val with wrong type
    verifySqlErr {
      this.eachImpl([DefValTestsErr2#]) |s| { /**/ }
    }
  }
}