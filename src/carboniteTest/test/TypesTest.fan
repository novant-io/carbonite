//
// Copyright (c) 2022, Novant LLC
// All Rights Reserved
//
// History:
//   3 Aug 2022  Andy Frank  Creation
//

using carbonite

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
      t.update(1, ["str":"foo bar"])

      r = t.get(1)
      verifyEq(r->str, "foo bar")
      verifyEq(r.getStr("str"), "foo bar")
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
    }
  }
}