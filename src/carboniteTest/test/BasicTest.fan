//
// Copyright (c) 2022, Novant LLC
// All Rights Reserved
//
// History:
//   4 Jul 2022  Andy Frank  Creation
//

using carbonite

*************************************************************************
** Employees
*************************************************************************

const class Employees : CTable
{
  override const Str name := "employees"
  // override const Type rec := EmployeeRec#
  override const CCol[] cols := [
    CCol("id",   Int#, [:]),
    CCol("name", Str#, [:]),
    CCol("pos",  Str#, [:]),
  ]
}

const class Employees2 : CTable
{
  override const Str name := "employees"
  override const CCol[] cols := [
    CCol("id",         Int#,  [:]),
    CCol("name",       Str#,  [:]),
    CCol("pos",        Str#,  [:]),
    CCol("new_column", Int?#, [:]),
  ]
}

const class EmployeesErr3 : CTable
{
  override const Str name := "employees"
  override const CCol[] cols := [
    CCol("id",         Int#, [:]),
    CCol("name",       Str#, [:]),
    CCol("pos",        Str#, [:]),
    CCol("new_column", Int#, [:]),  // non-null with no defval
  ]
}

const class EmployeesErr4 : CTable
{
  override const Str name := "employees"
  override const CCol[] cols := [
    CCol("id",         Int#, [:]),
    CCol("name",       Int#, [:]),   // schema mismatch
    CCol("pos",        Str#, [:]),
    CCol("new_column", Int#, [:]),
  ]
}

*************************************************************************
** BasicTest
*************************************************************************

class BasicTest : AbstractStoreTest
{
  Void testBasics()
  {
    eachImpl([Employees#]) |ds|
    {
      // empty
      verifyEq(ds.tables.size, 1)
      verifyEq(ds.table(Employees#).size, 0)
      verifyEq(ds.table(Employees#).listAll.size, 0)

      // add rows
      CTable e := ds.table(Employees#)
      e.create(["id":1, "name":"Ron Burgundy",          "pos":"lead"])
      e.create(["id":2, "name":"Veronica Corningstone", "pos":"lead"])
      e.create(["id":3, "name":"Brian Fantana",         "pos":"sports"])
      e.create(["id":4, "name":"Brick Tamland",         "pos":"weather"])
      verifyEq(e.size, 4)
      verifyEq(ds.table(Employees#).size, 4)
      verifyEq(ds.table(Employees#).listAll.size, 4)
      verifyEq(e.listAll[0]->name, "Ron Burgundy")
      verifyEq(e.listAll[2]->name, "Brian Fantana")

      // get
      verifyEq(e.get(2)->name, "Veronica Corningstone")
      verifyEq(e.get(5), null)

      // getBy
      verifyEq(e.getBy(["pos":"weather"])->name,      "Brick Tamland")
      verifyEq(e.getBy(["pos":"lead"])->name,         "Ron Burgundy")
      verifyEq(e.getBy(["pos":"lead", "id":2])->name, "Veronica Corningstone")

      // list
      verifyEq(e.listAll.size, 4)
      verifyEq(e.listBy(["pos":"lead"]).size, 2)
      verifyEq(e.listBy(["pos":"lead", "name":"Ron Burgundy"]).size, 1)
      verifyEq(e.listBy(["pos":"lead", "name":"No one"]).size, 0)

      // close and verify fail
      ds.close
      verifySqlErr { x := ds.table(Employees#).size }
    }

    // re-open new schema and nullable new col
    eachImpl([Employees2#]) |ds|
    {
      // add columns
      verifyEq(ds.tables.size, 1)
      e := ds.table(Employees2#)
      verifyEq(e.size, 4)
      verifyEq(e.listAll[0]->name, "Ron Burgundy")
      verifyEq(e.listAll[0]->new_column, null)

      // update and reset nullable column
      e.update(1, ["new_column":52])
      verifyEq(e.get(1)->new_column, 52)
      e.update(1, ["new_column":null])
      verifyEq(e.get(1)->new_column, null)

      // update row
      verifyEq(e.get(1)->name, "Ron Burgundy")
      e.update(1, ["name":"Ronnie Burgie"])
      verifyEq(e.get(1)->name, "Ronnie Burgie")
      e.update(4, ["name":"Bricky", "pos":"lead"])
      verifyEq(e.get(4)->name, "Bricky")
      verifyEq(e.get(4)->pos,  "lead")
    }

    // re-open new schema and non-null with no default value
    eachImplErr([EmployeesErr3#])

    // re-open with schema mismatch
    eachImplErr([EmployeesErr4#])

    // test deletes
    eachImpl([Employees#]) |ds|
    {
      // delete row
      e := ds.table(Employees#)
      verifyEq(e.size, 4)
      e.delete(1)
      verifyEq(e.size, 3)
      verifyEq(e.listAll[0]->name, "Veronica Corningstone")

      // delete row
      e.deleteBy(["name":"Veronica Corningstone"])
      verifyEq(e.size, 2)
      verifyEq(e.listAll[0]->name, "Brian Fantana")

      // delete row
      e.delete(3)
      verifyEq(e.size, 1)
      verifyEq(e.listAll[0]->name, "Bricky")

      // delete row
      e.delete(4)
      verifyEq(e.size, 0)
    }
  }
}