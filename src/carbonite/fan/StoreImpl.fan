//
// Copyright (c) 2022, Novant LLC
// All Rights Reserved
//
// History:
//   4 Jul 2022  Andy Frank  Creation
//

using concurrent
using [java] java.lang::Class as JClass

*************************************************************************
** StoreImpl
*************************************************************************

internal abstract const class StoreImpl
{
  ** Create SqlConn instance for given driver and connection info.
  protected SqlConn makeConn(Str jdbc, Str conn, Str? user, Str? pass)
  {
    // repload JDBC driver.
    try { JClass.forName(jdbc) }
    catch (Err err)
    {
      // nice message for jdbc driver not installed
      throw err.msg.contains("java.lang.ClassNotFoundException")
        ? Err("${jdbc} driver not found")
        : err
    }
    return SqlConn.open(conn, null, null)
  }

  // TODO FIXIT: move const-ness down into SqlXxx methods
  protected SqlConn conn() { (connRef.val as Unsafe).val }
  protected const AtomicRef connRef := AtomicRef(null)

  ** Execute sql querty with params and return results as Row[] list.
  protected Row[] exec(Str query, Str:Str params := [:])
  {
    res := execRaw(query, params)
    if (res is Row[]) return res
    if (res is Row) return [res]
    return Row#.emptyList
  }

  ** Execute sql querty with params and return results as Row[] list.
  protected Obj execRaw(Str query, Str:Str params := [:])
  {
    conn.sql(query).prepare.execute(params)
  }

  ** Verify sql schema matches current CStore schema.
  virtual This verifySchema(CStore store)
  {
    try
    {
      st := conn.meta.tables
      store.tables.each |t|
      {
        // verify table exists
        if (!st.contains(t.name)) throw Err("Missing table '${t.name}'")
      }
      return this
    }
    catch (Err err)
    {
      throw Err("Schema validation failed", err)
    }
  }

  ** Update sql schema to match CStore schema.
  virtual This updateSchema(CStore store)
  {
    try
    {
      store.tables.each |t|
      {
        // create table if not exist
        cols := t.cols.join(",") |c| { colToSql(c) }
        exec("create table if not exists ${t.name} (${cols})")

        // check if we need to add cols
        has := describeTable(t.name)
        t.cols.each |c|
        {
          cur := has.find |h| { h.startsWith("${c.name} ") }
          if (cur == null)
          {
            // add missing column
            exec("alter table ${t.name} add column ${colToSql(c)}")
          }
          else
          {
            // TODO: we do not not currently auto-update col schema
            // throw if schema mismatch
            if (colToSql(c) != cur) throw Err("Column schema mismatch '${c.name}'")
          }
        }

        // TODO: we do not currently auto-remove unused cols
      }
      return this
    }
    catch (Err err)
    {
      throw Err("Update schema failed", err)
    }
  }

  ** Return SQL schema for given 'CCol'.
  abstract Str colToSql(CCol col)

  ** Return SQL column schema from database for given table name.
  abstract Str[] describeTable(Str table)

  ** Effeciently return number of rows for given table.
  abstract Int tableSize(CTable table)

  ** Create a new record in sql database.
  virtual Void create(Str table, Str:Obj fields)
  {
    cols := fields.keys.join(",")
    vars := fields.keys.join(",") |n| { "@${n}" }
    exec("insert into ${table} (${cols}) values (${vars})", fields)
    // return new rec
  }

  ** Return result from select sql statement.
  virtual CRec[] select(Str table, Str cols)
  {
    // TODO FIXIT: fix sql to go directly -> CRec and nuke Row type
    exec("select * from ${table}").map |row| { CRec(row) }
  }
}