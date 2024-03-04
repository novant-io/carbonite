//
// Copyright (c) 2022, Novant LLC
// All Rights Reserved
//
// History:
//   30 Aug 2022  Andy Frank  Creation
//

using carbonite
using concurrent

*************************************************************************
** AbstractStoreTest
*************************************************************************

abstract class AbstractStoreTest : Test
{
  private File sqliteFile := Env.cur.tempDir + `test.db`

  private const Str dbname := "carbonite_test"
  private const Str dbuser := "carbonite_test"
  private const Str dbpass := "carbonite_pass"

  private const Bool sqlite   := true
  private const Bool postgres := true

  ** Start each test with fresh stores instances.
  override Void setup()
  {
    if (sqlite)
    {
      sqliteFile.delete
    }

    if (postgres)
    {
      // drop current test database
      psql_dropdb
      while (psql_dbexists) { Actor.sleep(10ms) }

      // create a fresh copy
      psql_createdb
      while (!psql_dbexists) { Actor.sleep(10ms) }
    }
  }

  **
  ** Convenience for `eachImplOpts` with no options.
  **
  Void eachImpl(Obj[] tables, |CStore store, Str impl| func)
  {
    eachImplOpts(tables, [:], func)
  }

  **
  ** Test each database impl. This function will reopen a new CStore
  ** instance to the existing database.  The database will be cleared
  ** and reset on entry to each test harness method.
  **
  Void eachImplOpts(Obj[] tables, Str:Obj opts, |CStore store, Str impl| func)
  {
    if (sqlite)
    {
      echo("   Impl: sqlite   $tables")
      store := CStore.openSqlite(sqliteFile, opts, tables)
      try func(store, "sqlite")
      finally store.close
    }

    if (postgres)
    {
      echo("   Impl: postgres $tables")
      store := CStore.openPostgres("localhost", dbname, dbuser, dbpass, opts, tables)
      try func(store, "postgres")
      finally store.close
    }
  }

  **
  ** Test each database impl throws SqlErr.
  **
  Void eachImplErr(Obj[] tables, Str:Obj opts := [:])
  {
    echo("   Impl: sqlite   $tables")
    verifySqlErr |->| { x := CStore.openSqlite(sqliteFile, opts, tables) }

    echo("   Impl: postgres $tables")
    verifySqlErr |->| { x := CStore.openPostgres("localhost", dbname, dbuser, dbpass, opts, tables) }
  }

  ** Verify that the given rec instance matches the given tag list.
  ** This method allows the rec to contain additional tags, only the
  ** 'tags' fields will be validated.
  protected Void verifyRec(CRec rec, Str:Obj tags)
  {
    tags.each |v,k|
    {
      verifyEq(rec.get(k), v)
    }
  }

  // TODO FIXIT: really just a temp fix until we cleanup ported sql stack
  protected Void verifySqlErr(|This| func)
  {
    verifyErr(Type.find("carbonite::SqlErr")) { func(this) }
  }

//////////////////////////////////////////////////////////////////////////
// psql utils
//////////////////////////////////////////////////////////////////////////

  ** Return 'true' if test database exists.
  private Bool psql_dbexists()
  {
    cmd  := "psql -lqt | cut -d \\| -f 1 | grep -qw ${dbname}"
    exit := Process { it.command=["bash", "-c", cmd] }.run.join
    return exit == 0
  }

  ** Drop the database if it exists.
  private Void psql_dropdb()
  {
    Process { it.command=["dropdb", "-f", "--if-exists", dbname] }.run
  }

  ** Create new blank database.
  private Void psql_createdb()
  {
    cmd := "psql postgres -U ${dbuser} -c 'create database ${dbname}'"
    Process { it.command=["bash", "-c", cmd] }.run
  }
}