//
// Copyright (c) 2026, Andy Frank
// Licensed under the MIT License
//
// History:
//   25 Feb 2026  Andy Frank  Creation
//

using carbonite

*************************************************************************
** Employees
*************************************************************************

const class WhereTests1 : CTable
{
  override const Str name := "where_tests_1"
  override const CCol[] cols := [
    CCol("id",    Int#,  ["primary_key":true, "auto_increment":true]),
    CCol("name",  Str#,  [:]),
    CCol("age",   Int#,  [:]),
    CCol("bday",  Date#, [:]),
  ]
}

*************************************************************************
** WhereTest
*************************************************************************

class WhereTest : AbstractStoreTest
{
  Void testBasics()
  {
    eachImpl([WhereTests1#]) |ds|
    {
      // add rows
      CTable t := ds.table(WhereTests1#)
      t.create(["name":"Ann", "age":25, "bday":Date("1999-03-15")])
      t.create(["name":"Bob", "age":30, "bday":Date("1994-07-22")])
      t.create(["name":"Cam", "age":35, "bday":Date("1989-11-08")])
      t.create(["name":"Dan", "age":40, "bday":Date("1984-01-30")])

      // simple equality
      r := t._listBy(["name":"Ann"])
      verifyEq(r.size, 1)
      verifyEq(r[0]->name, "Ann")

      // greater than
      r = t._listBy(["age >": 30])
      verifyEq(r.size, 2)
      verifyEq(r[0]->name, "Cam")
      verifyEq(r[1]->name, "Dan")

      // greater than or equal
      r = t._listBy(["age >=": 30])
      verifyEq(r.size, 3)
      verifyEq(r[0]->name, "Bob")
      verifyEq(r[1]->name, "Cam")
      verifyEq(r[2]->name, "Dan")

      // less than
      r = t._listBy(["age <": 35])
      verifyEq(r.size, 2)
      verifyEq(r[0]->name, "Ann")
      verifyEq(r[1]->name, "Bob")

      // less than or equal
      r = t._listBy(["age <=": 35])
      verifyEq(r.size, 3)
      verifyEq(r[0]->name, "Ann")
      verifyEq(r[1]->name, "Bob")
      verifyEq(r[2]->name, "Cam")

      // not equal
      r = t._listBy(["name !=": "Ann"])
      verifyEq(r.size, 3)
      verifyEq(r[0]->name, "Bob")
      verifyEq(r[1]->name, "Cam")
      verifyEq(r[2]->name, "Dan")

      // range: age > 25 and age < 40
      r = t._listBy(["age >": 25, "age <": 40])
      verifyEq(r.size, 2)
      verifyEq(r[0]->name, "Bob")
      verifyEq(r[1]->name, "Cam")

      // lower()
      r = t._listBy(["lower(name)": "ann"])
      verifyEq(r.size, 1)
      verifyEq(r[0]->name, "Ann")

      // in list
      r = t._listBy(["id": [1, 3]])
      verifyEq(r.size, 2)
      verifyEq(r[0]->name, "Ann")
      verifyEq(r[1]->name, "Cam")

      // TODO: I think we omit this if list is empty??
      // // empty in list
      // r = t._listBy(["id": Int[,]])
      // verifyEq(r.size, 0)

      // empty in list
      r = t._listBy(["id": [6,7,8]])
      verifyEq(r.size, 0)

      // combined: equality + operator
      r = t._listBy(["name": "Bob", "age >=": 30])
      verifyEq(r.size, 1)
      verifyEq(r[0]->name, "Bob")

      // combined: no match
      r = t._listBy(["name": "Ann", "age >": 30])
      verifyEq(r.size, 0)

      // duplicate map key: last value wins at map construction
      r = t._listBy(["age >": 25, "age >": 35])
      verifyEq(r.size, 1)
      verifyEq(r[0]->name, "Dan")

      // errs
      verifySqlErr { t._listBy(["age %%": 30]) }  // invalid operator
      verifySqlErr { t._listBy(["fake": 30])   }  // invalid column
    }

  }
}