//
// Copyright (c) 2022, Novant LLC
// All Rights Reserved
//
// History:
//   4 Jul 2022  Andy Frank  Creation
//

using carbonite

*************************************************************************
** BasicTest
*************************************************************************

class BasicTest : Test
{
  private File dbfile := Env.cur.tempDir + `test.db`
  override Void setup() { dbfile.delete }

  Void testBasics()
  {
    // empty
    ds := CStore.openSqlite(dbfile, [Employees#])
    verifyEq(ds.tables.size, 1)
    verifyEq(ds.table(Employees#).size, 0)
    verifyEq(ds.table(Employees#).listAll.size, 0)

    // add rows
    CTable e := ds.table(Employees#)
    e.create(["name":"Ron Burgundy",          "pos":"lead"])
    e.create(["name":"Veronica Corningstone", "pos":"lead"])
    e.create(["name":"Brian Fantana",         "pos":"sports"])
    e.create(["name":"Brick Tamland",         "pos":"weather"])
    verifyEq(e.size, 4)
    verifyEq(ds.table(Employees#).size, 4)
    verifyEq(ds.table(Employees#).listAll.size, 4)
    verifyEq(e.listAll[0]->name, "Ron Burgundy")
    verifyEq(e.listAll[2]->name, "Brian Fantana")

    // close and verify fail
    ds.close
    verifyErr(Type.find("carbonite::SqlErr")) { x := ds.table(Employees#).size }

    // add columns
    ds.close
    ds = CStore.openSqlite(dbfile, [Employees2#])
    verifyEq(ds.tables.size, 1)
    e = ds.table(Employees2#)
    verifyEq(e.size, 4)
    verifyEq(e.listAll[0]->name, "Ron Burgundy")
    verifyEq(e.listAll[0]->new_column, null)

    // add non-null column with no default value
    ds.close
    verifyErr(Err#) { ds = CStore.openSqlite(dbfile, [EmployeesErr3#]) }

    // schema mismatch
    ds.close
    verifyErr(Err#) { ds = CStore.openSqlite(dbfile, [EmployeesErr4#]) }
  }
}