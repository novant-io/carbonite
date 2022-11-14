//
// Copyright (c) 2022, Novant LLC
// All Rights Reserved
//
// History:
//   14 Nov 2022  Andy Frank  Creation
//

using carbonite

*************************************************************************
** ScopeTestA
*************************************************************************

const class ScopeTestA : CTable
{
  override const Str name := "scope_test_a"
  override const CCol[] cols := [
    CCol("id",   Int#, ["primary_key":true, "auto_increment":true]),
    CCol("name", Str#, [:]),
  ]
}

*************************************************************************
** ScopeTest
*************************************************************************

class ScopeTest : AbstractStoreTest
{
  Void test()
  {
    tables := [ScopeTestA#]
    eachImpl(tables) |s|
    {
      // verify empty
      CTable p := s.table(ScopeTestA#)
      verifyEq(p.size, 0)
    }
  }
}