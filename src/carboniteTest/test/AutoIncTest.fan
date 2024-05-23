//
// Copyright (c) 2022, Andy Frank
// Licensed under the MIT License
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

      // explicit id
      p.create(["id":7, "name":"Epsilon"])
      verifyEq(p.size, 4)
      verifyRec(p.listAll[0], ["id":1,  "name":"Alpha"])
      verifyRec(p.listAll[1], ["id":3,  "name":"Gamma"])
      verifyRec(p.listAll[2], ["id":4,  "name":"Delta"])
      verifyRec(p.listAll[3], ["id":7, "name":"Epsilon"])

      // TODO: should we prevent explict ids with auto_inc is specified?
      //       postgres will not account for out-of-order and generate a
      //       duplicate id and collision
      // verify next
      p.create(["name":"Zeta"])
      verifyEq(p.size, 5)
      verifyRec(p.listAll[3], ["id":7, "name":"Epsilon"])
      r := p.listAll[4]
      verify(r->id == 5 || r->id == 8)

      // p.create(["name":"x1"])
      // p.create(["name":"x2"])
      // p.create(["name":"x3"])
      // p.listAll.each |x| { echo("> $x") }
    }
  }
}