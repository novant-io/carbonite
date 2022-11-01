//
// Copyright (c) 2022, Novant LLC
// All Rights Reserved
//
// History:
//   30 Aug 2022  Andy Frank  Creation
//

using carbonite

*************************************************************************
** PrimaryKeyCompTestA
*************************************************************************

const class PrimaryKeyCompTestA : CTable
{
  override const Str name := "primary_key_comp_test_a"
  override const CCol[] cols := [
    CCol("foo_id", Int#, ["primary_key":true]),  // composite key
    CCol("bar_id", Int#, ["primary_key":true]),  // composite key
    CCol("name",   Str#, [:]),
  ]
}

*************************************************************************
** PrimaryKeyCompTest
*************************************************************************

class PrimaryKeyCompTest : AbstractStoreTest
{
  const Type[] tables := [PrimaryKeyCompTestA#]

  ** Test creating rows with primary keys.
  Void testCreate()
  {
    eachImpl(tables) |s|
    {
      // verify empty
      CTable p := s.table(PrimaryKeyCompTestA#)
      verifyEq(p.size, 0)

      // add row
      p.create(["foo_id":2, "bar_id":5, "name":"Alpha"])
      verifyEq(p.size, 1)
      verifyRec(p.listAll[0], ["foo_id":2, "bar_id":5, "name":"Alpha"])

      // add another unique row
      p.create(["foo_id":4, "bar_id":7, "name":"Beta"])
      verifyEq(p.size, 2)
      verifyRec(p.listAll[0], ["foo_id":2, "bar_id":5, "name":"Alpha"])
      verifyRec(p.listAll[1], ["foo_id":4, "bar_id":7, "name":"Beta"])

      // add dup foo_id/bar_id
      p.create(["foo_id":2, "bar_id":9, "name":"Dup Foo"])
      p.create(["foo_id":6, "bar_id":5, "name":"Dup Bar"])
      verifyEq(p.size, 4)
      verifyRec(p.listAll[0], ["foo_id":2, "bar_id":5, "name":"Alpha"])
      verifyRec(p.listAll[1], ["foo_id":4, "bar_id":7, "name":"Beta"])
      verifyRec(p.listAll[2], ["foo_id":2, "bar_id":9, "name":"Dup Foo"])
      verifyRec(p.listAll[3], ["foo_id":6, "bar_id":5, "name":"Dup Bar"])

      // add collision
      verifySqlErr { p.create(["foo_id":2, "bar_id":5, "name":"ERR"]) }
      verifySqlErr { p.create(["foo_id":6, "bar_id":5, "name":"ERR"]) }
    }
  }
}