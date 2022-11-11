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
  protected Row[] exec(Str query, [Str:Obj]? params := null)
  {
    res := execRaw(query, params)
    if (res is Row[]) return res
    if (res is Row) return [res]
    return Row#.emptyList
  }

  ** Execute sql querty with params and return results as Row[] list.
  protected Obj execRaw(Str query, [Str:Obj]? params := null)
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
        cstr := Str[,]
          .addAll(t.cols.map |c| { colToSql(store, c) })
          .addAll(t.constraints.map |c| { constraintToSql(c) })
          .join(",")
        exec("create table if not exists ${t.name} (${cstr})")

        // check if we need to add cols
        has := describeTable(t.name)
        t.cols.each |c|
        {
          cur := has.find |h| { h.startsWith("${c.name} ") }
          if (cur == null)
          {
            // add missing column
            exec("alter table ${t.name} add column ${colToSql(store, c)}")
          }
          else
          {
            // TODO: we do not not currently auto-update col schema
            // throw if schema mismatch
            if (colToSql(store, c) != cur)
            {
              x := colToSql(store, c)
              throw Err("Column schema mismatch '${c.name}': ${cur} != ${x}")
            }
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
  abstract Str colToSql(CStore store, CCol col)

  ** Return SQL schema for given 'CConstraint'.
  abstract Str constraintToSql(CConstraint c)

  ** Return SQL value for given Fantom value and 'CCol'.
  abstract Obj fanToSql(CCol col, Obj fan)

  ** Return Fantom value for given SQL value and 'CCol'.
  abstract Obj sqlToFan(CCol col, Obj fan)

  ** Return SQL column schema from database for given table name.
  abstract Str[] describeTable(Str table)

  ** Effeciently return number of rows for given table.
  abstract Int tableSize(CTable table)

  ** Create a new record in sql database.
  virtual Void create(CTable table, Str:Obj fields)
  {
    cols := fields.keys.join(",")
    vars := fields.keys.join(",") |n| { "@${n}" }
    exec("insert into ${table.name} (${cols}) values (${vars})", fieldsToSql(table, fields))
    // return new rec
  }

  ** Return result from select sql statement.
  virtual CRec[] select(CTable table, Str cols, [Str:Obj]? where := null)
  {
    sql := "select * from ${table.name}"
    if (where != null)
    {
      cond := StrBuf()
      where.each |v,n| { cond.join("${n} = @${n}", " and ") }
      sql += " where ${cond}"
    }
    // TODO FIXIT: fix sql to go directly -> CRec and nuke Row type
    return exec(sql, where).map |row|
    {
      // TODO FIXIT YOWZERS
      map := Str:Obj?[:]
      row.cols.each |rc|
      {
        c := table.cols.find |c| { c.name == rc.name }
        if (c == null) return
        v := row.get(rc)
        if (v != null) map[c.name] = sqlToFan(c, v)
      }
      return CRec(map)
    }
  }

  ** Update an existing record in sql database.
  virtual Void update(CTable table, Int id, Str:Obj? fields)
  {
    assign := fields.keys.join(",") |n| { "${n} = @${n}" }
    exec("update ${table.name} set ${assign} where id = ${id}", fieldsToSql(table, fields))
    // return new rec
  }

  ** Delete an existing record in sql database.
  virtual Void delete(CTable table, Int id)
  {
    exec("delete from ${table.name} where id = ${id}")
  }

  ** Delete an existing record based on given 'where' clause.
  virtual Void deleteBy(CTable table, Str:Obj where)
  {
    // TODO: make this DRY (see select)
    cond := StrBuf()
    where.each |v,n| { cond.join("${n} = @${n}", " and ") }
    exec("delete from ${table.name} where ${cond}", where)
  }

  // TODO FIXIT: this needs to happen in SqlUtil to avoid double mapping
  ** Convert fantom valus to sql compat values.
  private Str:Obj? fieldsToSql(CTable table, Str:Obj? fan)
  {
    // TODO FIXIT YOWZERS
    fan.map |f,n|
    {
      if (f == null) return null
      c := table.cols.find |c| { c.name == n }
      return fanToSql(c, f)
    }
  }
}