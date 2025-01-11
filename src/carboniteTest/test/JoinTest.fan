//
// Copyright (c) 2024, Andy Frank
// Licensed under the MIT License
//
// History:
//   29 Dec 2024  Andy Frank  Creation
//

using carbonite

*************************************************************************
** Tables
*************************************************************************

const class Orgs : CTable
{
  override const Str name := "orgs"
  override const CCol[] cols := [
    CCol("id",   Int#, [:]),
    CCol("name", Str#, [:]),
  ]
}

const class OrgMeta : CTable
{
  override const Str name := "org_meta"
  override const CCol[] cols := [
    CCol("org_id", Int#, [:]),
    CCol("code",   Str#, [:]),
    CCol("city",   Str#, [:]),
  ]
}

const class Users : CTable
{
  override const Str name := "users"
  override const CCol[] cols := [
    CCol("id",   Int#,    [:]),
    CCol("name", Str#,    [:]),
  ]
}

const class OrgUsers : CTable
{
  override const Str name := "org_users"
  override const CCol[] cols := [
    CCol("org_id",  Int#, [:]),
    CCol("user_id", Int#, [:]),
    CCol("role",    Str#, [:]),
  ]
}

*************************************************************************
** JoinTest
*************************************************************************

class JoinTest : AbstractStoreTest
{
  Void testBasics()
  {
    eachImpl([Orgs#, OrgMeta#, Users#, OrgUsers#]) |ds|
    {
      // init orgs
      orgs := ds.table(Orgs#)
      orgs.create(["id":1, "name":"ACME"])
      orgs.create(["id":2, "name":"GNB"])
      orgs.create(["id":3, "name":"KVWN"])

      // init org_meta
      meta := ds.table(OrgMeta#)
      meta.create(["org_id":1, "code":"A52", "city":"Holmdel"])
      meta.create(["org_id":2, "code":"B10", "city":"New York"])
      meta.create(["org_id":3, "code":"T37", "city":"San Diego"])

      // init users
      users := ds.table(Users#)
      users.create(["id":20, "name":"Ron Burgundy"])
      users.create(["id":21, "name":"Barney Stinson"])
      users.create(["id":22, "name":"Brick Tamland"])
      users.create(["id":23, "name":"Brian Fantana"])

      // init join
      join := ds.table(OrgUsers#)
      join.create(["org_id":2, "user_id":21, "role":"manager"])
      join.create(["org_id":3, "user_id":20, "role":"anchor"])
      join.create(["org_id":3, "user_id":23, "role":"sports"])

      // org join
      j := orgs._listJoin(OrgMeta#, "org_id")
      verifyEq(j.size, 3)
      verifyRec(j[0], ["id":1, "name":"ACME", "org_id":1, "code":"A52", "city":"Holmdel"])
      verifyRec(j[1], ["id":2, "name":"GNB",  "org_id":2, "code":"B10", "city":"New York"])
      verifyRec(j[2], ["id":3, "name":"KVWN", "org_id":3, "code":"T37", "city":"San Diego"])

      // org_users join
      // ACME
      j = users._listJoin(OrgUsers#, "user_id", ["org_id":1])
      verifyEq(j.size, 0)
      // GNB
      j = users._listJoin(OrgUsers#, "user_id", ["org_id":2])
      verifyEq(j.size, 1)
      verifyRec(j[0], ["id":21, "name":"Barney Stinson", "org_id":2, "user_id":21, "role":"manager"])
      // KVWN
      j = users._listJoin(OrgUsers#, "user_id", ["org_id":3])
      verifyRec(j[0], ["id":20, "name":"Ron Burgundy",  "org_id":3, "user_id":20, "role":"anchor"])
      verifyRec(j[1], ["id":23, "name":"Brian Fantana", "org_id":3, "user_id":23, "role":"sports"])

      // join with id_list (GNB + KVWN)
      j = users._listJoin(OrgUsers#, "user_id", ["org_id":[2,3]])
      verifyEq(j.size, 3)
      // sort by id since sqlite/postgres have different orders
      j.sort |a,b| { a->id <=> b->id }
      verifyRec(j[0], ["id":20, "name":"Ron Burgundy",   "org_id":3, "user_id":20, "role":"anchor"])
      verifyRec(j[1], ["id":21, "name":"Barney Stinson", "org_id":2, "user_id":21, "role":"manager"])
      verifyRec(j[2], ["id":23, "name":"Brian Fantana",  "org_id":3, "user_id":23, "role":"sports"])
    }
  }
}