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

class TypesTest : Test
{
  private File dbfile := Env.cur.tempDir + `test.db`
  override Void setup()
  {
    dbfile.delete
    this.db = CStore.openSqlite(dbfile, [Types#])
    this.t  = db.table(Types#)
    this.t.create(["id":1])
  }
  private CStore? db
  private CTable? t
  private CRec rec() { t.listAll.first }
  private Void update(Str:Obj map) { t.update(1, map) }

  Void testStr()
  {
    verifyEq(rec->str, null)
    update(["str":"foo bar"])
    verifyEq(rec->str, "foo bar")
    verifyEq(rec.getStr("str"), "foo bar")
  }

  Void testInt()
  {
    verifyEq(rec->int, null)
    update(["int":25])
    verifyEq(rec->int, 25)
    verifyEq(rec.getInt("int"), 25)
  }

  Void testDate()
  {
    verifyEq(rec->date, null)
    d := Date("2022-08-03")
    update(["date":d])
    verifyEq(rec->date, d)
    verifyEq(rec.getDate("date"), d)
  }

  Void testDateTime()
  {
    verifyEq(rec->datetime, null)
    dt := DateTime("2022-08-03T15:27:47-04:00 New_York")
    ny := TimeZone("New_York")
    update(["datetime":dt])
    verifyEq(rec->datetime, dt)
    verifyEq(rec->datetime.toStr, "2022-08-03T19:27:47Z UTC")
    verifyEq(rec.getDateTime("datetime"), dt)
    verifyEq(rec.getDateTime("datetime", ny).toStr, "2022-08-03T15:27:47-04:00 New_York")
  }
}