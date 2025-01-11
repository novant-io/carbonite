//
// Copyright (c) 2025, Andy Frank
// Licensed under the MIT License
//
// History:
//   11 Jan 2025  Andy Frank  Creation
//

*************************************************************************
** SqlExprTest
*************************************************************************

internal class SqlExprTest : Test
{

//////////////////////////////////////////////////////////////////////////
// Select
//////////////////////////////////////////////////////////////////////////

  Void testSelect()
  {
    s := SqlExpr.select("users", "*")
    verifyEq(s.expr, "select * from users")
    verifyEq(s.params, null)

    s = SqlExpr.select("users", "name,email")
    verifyEq(s.expr, "select name,email from users")
    verifyEq(s.params, null)
  }

  Void testSelectWhere()
  {
    // name = @name
    s := SqlExpr.select("users", "*", ["id":5])
    verifyEq(s.expr, "select * from users where id = @id")
    verifyEq(s.params, Str:Obj["id":5])

    // name = @name (2 params)
    s = SqlExpr.select("users", "*", ["enabled":true, "email":"bob@example.com"])
    verifyEq(s.expr, "select * from users where enabled = @enabled and email = @email")
    verifyEq(s.params, Str:Obj["enabled":true, "email":"bob@example.com"])

    // lower(name) = name
    s = SqlExpr.select("users", "*", ["lower(email)":"Bob@Example.com"])
    verifyEq(s.expr, "select * from users where lower(email) = @email_lower")
    verifyEq(s.params, Str:Obj["email_lower":"bob@example.com"])

    // name in (1,2,3)
    s = SqlExpr.select("users", "*", ["id":[1,2,3]])
    verifyEq(s.expr, "select * from users where id in (1,2,3)")
    verifyEq(s.params, Str:Obj[:])
  }

//////////////////////////////////////////////////////////////////////////
// Select Join
//////////////////////////////////////////////////////////////////////////

  Void testSelectJoin()
  {
    s := SqlExpr.selectJoin("users", "*", "orgs", "user_id")
    verifyEq(s.expr, "select * from users join orgs on (users.id = orgs.user_id)")
    verifyEq(s.params, null)

    s = SqlExpr.selectJoin("users", "name,email", "orgs", "user_id")
    verifyEq(s.expr, "select name,email from users join orgs on (users.id = orgs.user_id)")
    verifyEq(s.params, null)
  }

  Void testSelectJoinWhere()
  {
    // name = @name
    s := SqlExpr.selectJoin("users", "*", "orgs", "user_id", ["id":5])
    verifyEq(s.expr, "select * from users join orgs on (users.id = orgs.user_id and id = @id)")
    verifyEq(s.params, Str:Obj["id":5])

    // name = @name (2 params)
    s = SqlExpr.selectJoin("users", "*", "orgs", "user_id", ["enabled":true, "email":"bob@example.com"])
    verifyEq(s.expr, "select * from users join orgs on (users.id = orgs.user_id and enabled = @enabled and email = @email)")
    verifyEq(s.params, Str:Obj["enabled":true, "email":"bob@example.com"])

    // lower(name) = name
    s = SqlExpr.selectJoin("users", "*", "orgs", "user_id", ["lower(email)":"Bob@Example.com"])
    verifyEq(s.expr, "select * from users join orgs on (users.id = orgs.user_id and lower(email) = @email_lower)")
    verifyEq(s.params, Str:Obj["email_lower":"bob@example.com"])

    // name in (1,2,3)
    s = SqlExpr.selectJoin("users", "*", "orgs", "user_id", ["id":[1,2,3]])
    verifyEq(s.expr, "select * from users join orgs on (users.id = orgs.user_id and id in (1,2,3))")
    verifyEq(s.params, Str:Obj[:])
  }
}