//
// Copyright (c) 2022, Novant LLC
// All Rights Reserved
//
// History:
//   30 Aug 2022  Andy Frank  Creation
//

using carbonite

*************************************************************************
** PrimaryKeyTests
*************************************************************************

const class PrimaryKeyTests : CTable
{
  override const Str name := "primary_key_tests"
  override const CCol[] cols := [
    CCol("id",   Int#, ["primary_key":true]),
    CCol("name", Str#, [:]),
  ]
}

*************************************************************************
** PrimaryKeyTest
*************************************************************************

class PrimaryKeyTest : AbstractStoreTest
{
  const Type[] tables := [PrimaryKeyTests#]

  ** Test creating rows with primary keys.
  Void testCreate()
  {
    eachImpl(tables) |s|
    {
      // verify empty
      CTable p := s.table(PrimaryKeyTests#)
      verifyEq(p.size, 0)

      // add row using auto-increment
      p.create(["name":"Alpha"])
      verifyEq(p.size, 1)
      verifyRec(p.listAll[0], ["id":1, "name":"Alpha"])

      // add another row using auto-increment
      p.create(["name":"Beta"])
      verifyEq(p.size, 2)
      verifyRec(p.listAll[0], ["id":1, "name":"Alpha"])
      verifyRec(p.listAll[1], ["id":2, "name":"Beta"])

      // explicit id
      p.create(["id":5, "name":"Gamma"])
      verifyEq(p.size, 3)
      verifyRec(p.listAll[0], ["id":1, "name":"Alpha"])
      verifyRec(p.listAll[1], ["id":2, "name":"Beta"])
      verifyRec(p.listAll[2], ["id":5, "name":"Gamma"])

      // add another row using auto-increment
      p.create(["name":"Delta"])
      verifyEq(p.size, 4)
      verifyRec(p.listAll[0], ["id":1, "name":"Alpha"])
      verifyRec(p.listAll[1], ["id":2, "name":"Beta"])
      verifyRec(p.listAll[2], ["id":5, "name":"Gamma"])
      verifyRec(p.listAll[3], ["id":6, "name":"Delta"])
    }

    // reopen file and add additional rows
    eachImpl(tables) |s|
    {
      // verify existing
      CTable p := s.table(PrimaryKeyTests#)
      verifyEq(p.size, 4)

      // add another row using auto-increment
      p.create(["name":"Epsilon"])
      verifyEq(p.size, 5)
      verifyRec(p.listAll[0], ["id":1, "name":"Alpha"])
      verifyRec(p.listAll[1], ["id":2, "name":"Beta"])
      verifyRec(p.listAll[2], ["id":5, "name":"Gamma"])
      verifyRec(p.listAll[3], ["id":6, "name":"Delta"])
      verifyRec(p.listAll[4], ["id":7, "name":"Epsilon"])
    }
  }
}