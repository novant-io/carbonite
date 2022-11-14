//
// Copyright (c) 2022, Novant LLC
// All Rights Reserved
//
// History:
//   14 Nov 2022  Andy Frank  Creation
//

using carbonite

*************************************************************************
** AutoIncTestA
*************************************************************************

const class AutoIncTestA : CTable
{
  override const Str name := "auto_inc_test_a"
  override const CCol[] cols := [
    CCol("id",   Int#, ["primary_key":true, "auto_increment":true]),
    CCol("name", Str#, [:]),
  ]
}

*************************************************************************
** AutoIncTest
*************************************************************************

class AutoIncTest : AbstractStoreTest
{
  ** Test using column constraint.
  Void testCol()
  {
    tables := [AutoIncTestA#]
    eachImpl(tables) |s|
    {
      // verify empty
      CTable p := s.table(AutoIncTestA#)
      verifyEq(p.size, 0)

      // add row
      p.create(["name":"Alpha"])
      verifyEq(p.size, 1)
      verifyRec(p.listAll[0], ["id":1, "name":"Alpha"])

      // add another row
      p.create(["name":"Beta"])
      verifyEq(p.size, 2)
      verifyRec(p.listAll[0], ["id":1, "name":"Alpha"])
      verifyRec(p.listAll[1], ["id":2, "name":"Beta"])

      // remove row
      p.delete(2)
      verifyEq(p.size, 1)
      verifyRec(p.listAll[0], ["id":1, "name":"Alpha"])

      // add row and check skip to 3
      p.create(["name":"Gamma"])
      verifyEq(p.size, 2)
      verifyRec(p.listAll[0], ["id":1, "name":"Alpha"])
      verifyRec(p.listAll[1], ["id":3, "name":"Gamma"])

      // err collision
      verifySqlErr { p.create(["id":3, "name":"ERR"]) }
    }

    // reopen and add additional rows
    eachImpl(tables) |s|
    {
      // verify existing
      CTable p := s.table(AutoIncTestA#)
      verifyEq(p.size, 2)

      // add another row using auto-increment
      p.create(["name":"Delta"])
      verifyEq(p.size, 3)
      verifyRec(p.listAll[0], ["id":1, "name":"Alpha"])
      verifyRec(p.listAll[1], ["id":3, "name":"Gamma"])
      verifyRec(p.listAll[2], ["id":4, "name":"Delta"])
    }
  }
}