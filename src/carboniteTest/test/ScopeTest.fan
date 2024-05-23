//
// Copyright (c) 2022, Andy Frank
// Licensed under the MIT License
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
    CCol("id",       Int#, ["primary_key":true, "auto_increment":true]),
    CCol("name",     Str#, [:]),
    CCol("proj_id",  Int#, [:]),
    CCol("scope_id", Int#, ["auto_increment":true, "scoped_by":"proj_id"]),
  ]

  // not needed for test; just a sanity check
  override const CConstraint[] constraints := [
    CConstraint.unique(["proj_id", "scope_id"])
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
      CTable t := s.table(ScopeTestA#)
      verifyEq(t.size, 0)

      // add a few rows
      t.create(["name":"Alpha", "proj_id":1])
      t.create(["name":"Beta",  "proj_id":1])
      t.create(["name":"Gamma", "proj_id":1])
      verifyEq(t.size, 3)
      verifyEq(t.get(1)->scope_id, 1)
      verifyEq(t.get(2)->scope_id, 2)
      verifyEq(t.get(3)->scope_id, 3)

      // add different proj
      t.create(["name":"Zeta",   "proj_id":8])
      t.create(["name":"Theta",  "proj_id":8])
      t.create(["name":"Iota",   "proj_id":8])
      verifyEq(t.size, 6)
      verifyEq(t.get(4)->scope_id, 1)
      verifyEq(t.get(5)->scope_id, 2)
      verifyEq(t.get(6)->scope_id, 3)

      // now another one proj:1
      t.create(["name":"Delta", "proj_id":1])
      verifyEq(t.size, 7)
      verifyEq(t.get(7)->scope_id, 4)
    }
  }
}