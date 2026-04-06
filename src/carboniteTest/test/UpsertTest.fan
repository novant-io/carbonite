//
// Copyright (c) 2026, Andy Frank
// Licensed under the MIT License
//
// History:
//   6 Apr 2026  Andy Frank  Creation
//

using carbonite

*************************************************************************
** UpsertTests
*************************************************************************

const class UpsertTests : CTable
{
  override const Str name := "upsert_tests"
  override const CCol[] cols := [
    CCol("id",    Int#,   ["primary_key":true, "auto_increment":true]),
    CCol("code",  Str#,   [:]),
    CCol("name",  Str#,   [:]),
    CCol("date",  Date?#, [:]),
    CCol("score", Int#,   [:]),
  ]
  override const CConstraint[] constraints := [
    CConstraint.unique(["code", "name"])
  ]
}

*************************************************************************
** UpsertTest
*************************************************************************

class UpsertTest : AbstractStoreTest
{
  const Type[] tables := [UpsertTests#]
  Void test()
  {
    d1 := Date("2026-01-01")
    d2 := Date("2026-02-01")
    d3 := Date("2026-03-01")
    eachImpl(tables) |s|
    {
      CTable t := s.table(UpsertTests#)

      // insert fresh row
      t.upsert(["code":"a", "name":"Alpha", "date":d1, "score":10])
      recs := t.listAll
      verifyEq(recs.size, 1)
      verifyEq(recs[0]->code,  "a")
      verifyEq(recs[0]->name,  "Alpha")
      verifyEq(recs[0]->date,  d1)
      verifyEq(recs[0]->score, 10)

      // insert second unique row
      t.upsert(["code":"b", "name":"Beta", "date":d2, "score":20])
      recs = t.listAll
      verifyEq(recs.size, 2)

      // upsert existing row - updates date and score, code+name unchanged
      t.upsert(["code":"a", "name":"Alpha", "date":d3, "score":99])
      recs = t.listAll
      verifyEq(recs.size, 2)
      r := recs.find |x| { x->code == "a" }
      verifyEq(r->name,  "Alpha")
      verifyEq(r->date,  d3)
      verifyEq(r->score, 99)

      // upsert preserves auto-generated id on update
      origId := r->id
      t.upsert(["code":"a", "name":"Alpha", "date":d1, "score":1])
      recs = t.listAll
      verifyEq(recs.size, 2)
      r = recs.find |x| { x->code == "a" && x->name == "Alpha" }
      verifyEq(r->id, origId)
      verifyEq(r->score, 1)

      // verify other row untouched
      r = recs.find |x| { x->code == "b" }
      verifyEq(r->name,  "Beta")
      verifyEq(r->date,  d2)
      verifyEq(r->score, 20)

      // upsert with only some fields changing
      t.upsert(["code":"b", "name":"Beta", "date":d3, "score":20])
      recs = t.listAll
      verifyEq(recs.size, 2)
      r = recs.find |x| { x->code == "b" }
      verifyEq(r->date,  d3)
      verifyEq(r->score, 20)

      // upsert with new conflict combo inserts
      t.upsert(["code":"a", "name":"Beta", "date":d1, "score":50])
      recs = t.listAll
      verifyEq(recs.size, 3)

      // upsert third row by conflict match
      t.upsert(["code":"a", "name":"Beta", "date":d2, "score":75])
      recs = t.listAll
      verifyEq(recs.size, 3)
      r = recs.find |x| { x->code == "a" && x->name == "Beta" }
      verifyEq(r->date,  d2)
      verifyEq(r->score, 75)

      // upsert with null value
      t.upsert(["code":"c", "name":"Charlie", "date":d1, "score":30])
      recs = t.listAll
      verifyEq(recs.size, 4)
      t.upsert(["code":"c", "name":"Charlie", "date":null, "score":30])
      recs = t.listAll
      r = recs.find |x| { x->code == "c" }
      verifyEq(r->date,  null)
      verifyEq(r->score, 30)
    }
  }
}