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
  override Void setup() { dbfile.delete }

  Void testTypes()
  {
    d  := Date("2022-08-03")
    dt := DateTime("2022-08-03T15:27:47-04:00 New_York")

    ds := CStore.openSqlite(dbfile, [Types#])
    ds.table(Types#).create([
      "str":  "foobar",
      "int":  5,
      // "date": d,
    ])

    r := ds.table(Types#).listAll[0]
    verifyEq(r->str,  "foobar")
    verifyEq(r->int,  5)
    // verifyEq(r->date, d)
  }
}