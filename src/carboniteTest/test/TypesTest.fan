//
// Copyright (c) 2022, Novant LLC
// All Rights Reserved
//
// History:
//   3 Aug 2022  Andy Frank  Creation
//

using carbonite

*************************************************************************
** Types
*************************************************************************

const class Types : CTable
{
  override const Str name := "types"
  override const CCol[] cols := [
    CCol("id",       Int#,       [:]),
    CCol("str",      Str?#,      [:]),
    CCol("bool",     Bool?#,     [:]),
    CCol("int",      Int?#,      [:]),
    CCol("int_list", Int[]?#,    [:]),
    CCol("date",     Date?#,     [:]),
    CCol("datetime", DateTime?#, [:]),
  ]
}

*************************************************************************
** TypesTest
*************************************************************************

class TypesTest : AbstractStoreTest
{
  const Type[] tables := [Types#]

  private File dbfile := Env.cur.tempDir + `test.db`
  override Void setup()
  {
    super.setup
    eachImpl(tables) |s| { s.table(Types#).create(["id":1]) }
  }

  Void testStr()
  {
    eachImpl(tables) |s|
    {
      t := s.table(Types#)
      r := t.get(1)
      verifyEq(r->str, null)

      // update
      t.update(1, ["str":"foo bar"])
      r = t.get(1)
      verifyEq(r->str, "foo bar")
      verifyEq(r.getStr("str"), "foo bar")

      // create
      t.create(["id":2, "str":"bar zar"])
      r = t.get(2)
      verifyEq(r->str, "bar zar")
      verifyEq(r.getStr("str"), "bar zar")
    }
  }

  Void testBool()
  {
    eachImpl(tables) |s|
    {
      t := s.table(Types#)
      r := t.get(1)
      verifyEq(r->bool, null)

      t.update(1, ["bool":true])
      r = t.get(1)
      verifyEq(r->bool, true)
      verifyEq(r.getBool("bool"), true)

      t.update(1, ["bool":false])
      r = t.get(1)
      verifyEq(r->bool, false)
      verifyEq(r.getBool("bool"), false)

      t.update(1, ["bool":null])
      r = t.get(1)
      verifyEq(r->bool, null)
      verifyEq(r.getBool("bool"), null)

      // create
      t.create(["id":2, "bool":true])
      r = t.get(2)
      verifyEq(r->bool, true)
      verifyEq(r.getBool("bool"), true)
    }
  }

  Void testInt()
  {
    eachImpl(tables) |s|
    {
      t := s.table(Types#)
      r := t.get(1)
      verifyEq(r->int, null)
      t.update(1, ["int":25])

      r = t.get(1)
      verifyEq(r->int, 25)
      verifyEq(r.getInt("int"), 25)

      // create
      t.create(["id":2, "bool":true])
      r = t.get(2)
      verifyEq(r->bool, true)
      verifyEq(r.getBool("bool"), true)
    }
  }

  Void testIntList()
  {
    eachImpl(tables) |s|
    {
      t := s.table(Types#)
      r := t.get(1)
      verifyEq(r->int, null)

      t.update(1, ["int_list":[1,3,7,9]])
      r = t.get(1)
      verifyEq(r->int_list, [1,3,7,9])
      verifyEq(r.getIntList("int_list"), [1,3,7,9])

      t.update(1, ["int_list":[,]])
      r = t.get(1)
      verifyEq(r->int_list, Int[,])
      verifyEq(r.getIntList("int_list"), Int[,])

      // create
      t.create(["id":2, "int_list":[1,2,3]])
      r = t.get(2)
      verifyEq(r->int_list, [1,2,3])
      verifyEq(r.getIntList("int_list"), [1,2,3])
    }
  }

  Void testDate()
  {
    eachImpl(tables) |s|
    {
      t := s.table(Types#)
      r := t.get(1)
      d := Date("2022-08-03")

      verifyEq(r->date, null)
      t.update(1, ["date":d])

      r = t.get(1)
      verifyEq(r->date, d)
      verifyEq(r.getDate("date"), d)

      // create
      d2 := Date("2024-03-04")
      t.create(["id":2, "date":d2])
      r = t.get(2)
      verifyEq(r->date, d2)
      verifyEq(r.getDate("date"), d2)
    }
  }

  Void testDateTime()
  {
    eachImpl(tables) |s|
    {
      dt := DateTime("2022-08-03T15:27:47-04:00 New_York")
      ny := TimeZone("New_York")

      t := s.table(Types#)
      r := t.get(1)
      verifyEq(r->datetime, null)

      t.update(1, ["datetime":dt])
      r = t.get(1)
      verifyEq(r->datetime, dt)
      verifyEq(r->datetime.toStr, "2022-08-03T19:27:47Z UTC")
      verifyEq(r.getDateTime("datetime"), dt)
      verifyEq(r.getDateTime("datetime", ny).toStr, "2022-08-03T15:27:47-04:00 New_York")

      // create
      dt2 := DateTime("2024-03-04T10:43:00-05:00 New_York")
      t.create(["id":2, "datetime":dt2])
      r = t.get(2)
      verifyEq(r->datetime, dt2)
      verifyEq(r->datetime.toStr, "2024-03-04T15:43:00Z UTC")
      verifyEq(r.getDateTime("datetime"), dt2)
      verifyEq(r.getDateTime("datetime", ny).toStr, "2024-03-04T10:43:00-05:00 New_York")
    }
  }
}