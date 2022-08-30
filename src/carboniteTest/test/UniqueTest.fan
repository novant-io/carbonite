//
// Copyright (c) 2022, Novant LLC
// All Rights Reserved
//
// History:
//   30 Aug 2022  Andy Frank  Creation
//

using carbonite

*************************************************************************
** UniqueTests
*************************************************************************

const class UniqueTests : CTable
{
  override const Str name := "unique_tests"
  override const CCol[] cols := [
    CCol("id",   Int#,  ["unique":true]),
    CCol("name", Str#,  ["unique":true]),
    CCol("date", Date#, ["unique":true]),
  ]
}

*************************************************************************
** PrimaryKeyTest
*************************************************************************

class UniqueTest : AbstractStoreTest
{
  const Type[] tables := [UniqueTests#]

  Void test()
  {
    d1 := Date(2022, Month.aug, 25)
    d2 := Date(2022, Month.aug, 26)
    d3 := Date(2022, Month.aug, 27)

    eachImpl(tables) |s|
    {
      // add initial row
      CTable u := s.table(UniqueTests#)
      u.create(["id":1, "name":"Alpha", "date":d1])

      // add another unqiue row
      u.create(["id":2, "name":"Beta", "date":d2])

      // dup cols
      verifySqlErr { u.create(["id":1, "name":"Gamma", "date":d3]) }
      verifySqlErr { u.create(["id":5, "name":"Alpha", "date":d3]) }
      verifySqlErr { u.create(["id":5, "name":"Gamma", "date":d1]) }

      // test update unique
      u.update(1, ["name":"Alpha1"])
      u.update(2, ["date":d3])

      // dup cols
      verifySqlErr { u.update(1, ["name":"Beta"]) }
      verifySqlErr { u.update(2, ["date":d1]) }
    }
  }
}