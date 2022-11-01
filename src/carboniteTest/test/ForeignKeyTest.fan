//
// Copyright (c) 2022, Novant LLC
// All Rights Reserved
//
// History:
//   1 Nov 2022  Andy Frank  Creation
//

using carbonite

*************************************************************************
** ForeignKeyTests
*************************************************************************

const class FK_Orgs : CTable
{
  override const Str name := "fk_orgs"
  override const CCol[] cols := [
    CCol("id",   Int#, ["primary_key":true]),
    CCol("name", Str#, [:]),
  ]
}

const class FK_Users : CTable
{
  override const Str name := "fk_users"
  override const CCol[] cols := [
    CCol("id",     Int#, ["primary_key":true]),
    CCol("name",   Str#, [:]),
    CCol("org_id", Int#, ["foreign_key":FK_Orgs#]),
  ]
}

*************************************************************************
** ForeignKeyTest
*************************************************************************

class ForeignKeyTest : AbstractStoreTest
{
  const Type[] tables := [FK_Orgs#, FK_Users#]

  ** Test creating rows with primary keys.
  Void testCreate()
  {
    eachImpl(tables) |s|
    {
      // verify empty
      CTable o := s.table(FK_Orgs#)
      CTable u := s.table(FK_Users#)
      verifyEq(o.size, 0)
      verifyEq(u.size, 0)

      // add org
      o.create(["name":"Org 1"])
      verifyEq(o.size, 1)

      // add an org and user
      u.create(["name":"Bobby", "org_id":1])
      verifyEq(u.size, 1)

      // test no org_id exists
      verifySqlErr { u.create(["name":"ERR", "org_id":3]) }
    }
  }
}