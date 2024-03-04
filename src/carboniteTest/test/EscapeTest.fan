//
// Copyright (c) 2022, Novant LLC
// All Rights Reserved
//
// History:
//   17 Nov 2022  Andy Frank  Creation
//

using carbonite

*************************************************************************
** EscapeTestA
*************************************************************************

const class EscapeTestA : CTable
{
  override const Str name := "primary_key_test_a"
  override const CCol[] cols := [
    CCol("id",   Int#, ["primary_key":true]),
    CCol("desc", Str#, [:]),
  ]
}

*************************************************************************
** EscapeTestA
*************************************************************************

class EscapeTest : AbstractStoreTest
{
  ** Test column name escaping.
  Void test()
  {
    tables := [EscapeTestA#]
    eachImpl(tables) |s|
    {
      // NOTE: this tests that desc gets wrapped as "desc" in sql statements

      // verify empty
      CTable p := s.table(EscapeTestA#)
      verifyEq(p.size, 0)

      // add row
      p.create(["id":1, "desc":"Hello"])
      verifyEq(p.size, 1)
      verifyRec(p.get(1), ["id":1, "desc":"Hello"])

      // update
      p.update(1, ["desc":"World"])
      verifyRec(p.get(1), ["id":1, "desc":"World"])
    }
  }
}