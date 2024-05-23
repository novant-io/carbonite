//
// Copyright (c) 2022, Andy Frank
// Licensed under the MIT License
//
// History:
//   30 Aug 2022  Andy Frank  Creation
//

using carbonite

*************************************************************************
** UniqueCompTests
*************************************************************************

const class UniqueCompTests : CTable
{
  override const Str name := "unique_comp_tests"
  override const CCol[] cols := [
    CCol("id",   Int#,  [:]),
    CCol("name", Str#,  [:]),
  ]
  override const CConstraint[] constraints := [
    CConstraint.unique(["id", "name"])
  ]
}

*************************************************************************
** UniqueCompTest
*************************************************************************

class UniqueCompTest : AbstractStoreTest
{
  const Type[] tables := [UniqueCompTests#]

  Void test()
  {
    eachImpl(tables) |s|
    {
      // add initial row
      CTable u := s.table(UniqueCompTests#)
      u.create(["id":1, "name":"Alpha"])
      verifyEq(u.size, 1)

      // add another unqiue row
      u.create(["id":2, "name":"Beta"])
      verifyEq(u.size, 2)

      // add repeat but not dups
      u.create(["id":2, "name":"Alpha2"])
      u.create(["id":1, "name":"Beta2"])
      verifyEq(u.size, 4)

      // dup
      verifySqlErr { u.create(["id":1, "name":"Alpha"]) }
      verifySqlErr { u.create(["id":2, "name":"Beta"]) }

      // test update unique
      verifyEq(u.size, 4)
      u.create(["id":3, "name":"Gamma"])
      u.update(3, ["name":"Gamma~"])
      verifyEq(u.size, 5)
    }
  }
}