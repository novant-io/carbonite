//
// Copyright (c) 2022, Novant LLC
// All Rights Reserved
//
// History:
//   30 Aug 2022  Andy Frank  Creation
//

using carbonite

*************************************************************************
** PrimaryKeyTestA
*************************************************************************

const class PrimaryKeyTestA : CTable
{
  override const Str name := "primary_key_test_a"
  override const CCol[] cols := [
    CCol("id",   Int#, ["primary_key":true]),
    CCol("name", Str#, [:]),
  ]
}

*************************************************************************
** PrimaryKeyTestB
*************************************************************************

const class PrimaryKeyTestB : CTable
{
  override const Str name := "primary_key_test_b"
  override const CCol[] cols := [
    CCol("id",   Int#, [:]),
    CCol("name", Str#, [:]),
  ]
  override const CConstraint[] constraints := [
    CConstraint.primaryKey(["id"])
  ]
}

*************************************************************************
** PrimaryKeyTest
*************************************************************************

class PrimaryKeyTest : AbstractStoreTest
{
  ** Test using column constraint.
  Void testCol()
  {
    tables := [PrimaryKeyTestA#]
    eachImpl(tables) |s|
    {
      // verify empty
      CTable p := s.table(PrimaryKeyTestA#)
      verifyEq(p.size, 0)

      // add row
      p.create(["id":1, "name":"Alpha"])
      verifyEq(p.size, 1)
      verifyRec(p.listAll[0], ["id":1, "name":"Alpha"])

      // add another row
      p.create(["id":2, "name":"Beta"])
      verifyEq(p.size, 2)
      verifyRec(p.listAll[0], ["id":1, "name":"Alpha"])
      verifyRec(p.listAll[1], ["id":2, "name":"Beta"])

      // err collision
      verifySqlErr { p.create(["id":2, "name":"ERR"]) }
    }
  }

  ** Test using table level constraint.
  Void testConstraint()
  {
    tables := [PrimaryKeyTestB#]
    eachImpl(tables) |s|
    {
      // verify empty
      CTable p := s.table(PrimaryKeyTestB#)
      verifyEq(p.size, 0)

      // add row
      p.create(["id":1, "name":"Alpha"])
      verifyEq(p.size, 1)
      verifyRec(p.listAll[0], ["id":1, "name":"Alpha"])

      // add another row
      p.create(["id":2, "name":"Beta"])
      verifyEq(p.size, 2)
      verifyRec(p.listAll[0], ["id":1, "name":"Alpha"])
      verifyRec(p.listAll[1], ["id":2, "name":"Beta"])

      // err collision
      verifySqlErr { p.create(["id":2, "name":"ERR"]) }
    }
  }
}