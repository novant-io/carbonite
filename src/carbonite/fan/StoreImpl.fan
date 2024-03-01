//
// Copyright (c) 2022, Novant LLC
// All Rights Reserved
//
// History:
//   4 Jul 2022  Andy Frank  Creation
//

using concurrent
using [java] java.lang::Class as JClass
using [java] java.sql::Connection as JConnection
using [java] java.sql::Statement as JStatement

*************************************************************************
** StoreImpl
*************************************************************************

internal abstract const class StoreImpl
{
  ** Post contructor callback.
  protected Void init()
  {
    // sanity checks
    if (driver  == null) throw ArgErr("driver not configured")
    if (connStr == null) throw ArgErr("connStr not configured")
    if (opts    == null) throw ArgErr("opts not configured")

    // cache opts and open
    this.autoReopen.val = opts["auto_reopen"] == true
    this.openConn
  }

  ** JDBC driver name (must be set in subclass)
  protected const Str? driver

  ** JDBC connection string (must be set in subclass)
  protected const Str? connStr

  ** Options (must be set in subclass)
  protected const [Str:Obj]? opts
  private const AtomicBool autoReopen := AtomicBool(false)

  ** Write lock.
  private const Lock writeLock := Lock.makeReentrant

  ** Create SqlConn instance for given driver and connection info.
  protected Void openConn()
  {
    // preload JDBC driver.
    try { JClass.forName(driver) }
    catch (Err err)
    {
      // nice message for jdbc driver not installed
      throw err.msg.contains("java.lang.ClassNotFoundException")
        ? Err("${driver} driver not found")
        : err
    }

    // open conn
    conn := SqlConn.open(connStr, null, null) // user, pass)
    this.connRef.val = Unsafe(conn)
  }

  // TODO FIXIT: move const-ness down into SqlXxx methods
  protected SqlConn conn() { (connRef.val as Unsafe).val }
  protected const AtomicRef connRef := AtomicRef(null)

  ** Return 'true' if backing connection is closed.
  protected Bool isClosed()
  {
    if (connRef.val == null) return true
    return conn.isClosed
  }

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
    // check for auto_reopen
    if (conn.isClosed && autoReopen.val) openConn

    // exec query
    return conn.sql(query).prepare.execute(params)
  }

  ** Execute sql query with params inside write lock.
  protected Obj execWrite(Str query, [Str:Obj]? params := null)
  {
    // check for auto_reopen
    if (conn.isClosed && autoReopen.val) openConn

    // exec write
    if (!writeLock.tryLock(10sec)) throw InterruptedErr("Lock acquire failed")
    try { return conn.sql(query).prepare.execute(params) }
    finally { writeLock.unlock }
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
        has := describeTable(t)
        t.cols.each |c|
        {
          cur := has.find |h| { h.startsWith("\"${c.name}\" ") }
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

        // TODO FIXIT: we need to test table-level constraints too

        // TODO: we do not currently auto-remove unused cols
      }
      return this
    }
    catch (Err err)
    {
      throw SqlErr("Update schema failed", err)
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
  abstract Str[] describeTable(CTable table)

  ** Effeciently return number of rows for given table.
  virtual Int tableSize(CTable table)
  {
    r := exec("select count(1) from ${table.name}").first
    return r.get(r.cols.first)
  }

  ** Create a new record in sql database and return new id.
  virtual Int create(CTable table, Str:Obj fields)
  {
    cols := fields.keys.join(",") |c| { "\"${c}\"" }
    vars := fields.keys.join(",") |n| { "@${n}" }
    res := execRaw("insert into ${table.name} (${cols}) values (${vars})", fieldsToSql(table, fields))
    // TODO: for now we require an id column
    Int id := (res as List).first
    return id
  }

  ** Create a new record in sql database and return new id.
  virtual Int[] createAll(CTable table, Str[] cols, [Str:Obj?][] rows)
  {
    // TODO: looks like psql can use COPY for much better performance?

    // TODO: test perf with setAutoCommit(false)
    // TODO: setTransactionIsolation?

    // TODO: do we auto break up rows into batch sizes?
    //  - is this a CStore config?
    //  - or does this method take an `opts` argument?

    // TODO: we can probably just nuke all the peer stuff and
    // directly use the JDBC APIs; a lot unneeded overhead down
    // in that layer

    // TODO: for now do all work inside the lock; but I think
    // on the execute needs to go here; add a concurrent unit
    // test to flush this out
    if (!writeLock.tryLock(10sec)) throw InterruptedErr("Lock acquire failed")
    try
    {
      // build sql statemen
      sql := StrBuf()
      sql.add("insert into ").add(table.name).add(" (")
      cols.each |c,i|
      {
        if (i > 0) sql.addChar(',')
        sql.add(c)
      }
      sql.add(") values(")
      cols.each |c,i|
      {
        if (i > 0) sql.addChar(',')
        sql.addChar('?')
      }
      sql.addChar(')')
      // echo("> $sql")

      // get stmt instance
      JConnection? jconn := conn->java
      ps := jconn.prepareStatement(sql.toStr, JStatement.RETURN_GENERATED_KEYS)

      // batch add
      vals := List.makeObj(cols.size)
      rows.each |row|
      {
        vals.clear
        cols.each |c,i| { ps.setObject(i+1, row[c]) }
        ps.addBatch
      }

      // execute then collect ids
      ps.executeBatch
// TODO: this is not impl on sqlite; how should this work?
      ids := List.make(Int#, rows.size)
      rs  := ps.getGeneratedKeys
      while (rs.next) { ids.add(rs.getLong(1)) }
      return ids
    }
    finally { writeLock.unlock }
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
    assign := fields.keys.join(",") |n| { "\"${n}\" = @${n}" }
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