//
// Copyright (c) 2022, Novant LLC
// All Rights Reserved
//
// History:
//   4 Jul 2022  Andy Frank  Creation
//

*************************************************************************
** SqliteStoreImpl
*************************************************************************

internal const class SqliteStoreImpl : StoreImpl
{
  new make(File file)
  {
    if (!file.exists) file.create
    conn := makeConn("org.sqlite.JDBC", "jdbc:sqlite:${file.osPath}", null, null)
    this.connRef.val = Unsafe(conn)
  }

  override Str[] describeTable(Str table)
  {
    stmt  := "select * from sqlite_schema where name = @table"
    rows  := (Row[])conn.sql(stmt).prepare.execute(["table":table])
    sql   := rows.first->sql.toStr
    return sql[sql.index("(")+1..-2].split(',')
  }

  override Int tableSize(CTable table)
  {
    r := exec("select count(1) from ${table.name}").first
    return r.get(r.cols.first)
  }

  override Str colToSql(CCol col)
  {
    sql := StrBuf()
    sql.add(col.name)

    // base type
    switch (col.type.toNonNullable)
    {
      case Str#:      sql.join("text",    " ")
      case Int#:      sql.join("integer", " ")
      case Date#:     sql.join("integer", " ")
      case DateTime#: sql.join("integer", " ")
      default:        throw ArgErr("Unsupported col type '${col.type}'")
    }

    // nullable
    if (!col.type.isNullable) sql.join("not null", " ")

    // primary key
    if (col.primaryKey) sql.join("primary key", " ")

    // unique
    if (col.meta["unique"] == true) sql.join("unique", " ")

    return sql.toStr
  }

  override Obj fanToSql(CCol col, Obj fan)
  {
    // short-circuit if types do not match
    if (col.type.toNonNullable != fan.typeof.toNonNullable)
      throw ArgErr("Invalid type: ${fan} (${fan.typeof} != ${col.type})")

    switch (col.type.toNonNullable)
    {
      case Str#:  return fan
      case Int#:  return fan
      case Date#:
        Date d := fan
        y := d.year
        m := d.month.ordinal+1
        a := d.day
        return y.shiftl(16).or(m.shiftl(8)).or(a)

      case DateTime#:
        DateTime d := fan
        return d.toUtc.ticks

      default: throw ArgErr("Unsupported col type '${col.type}'")
    }
  }

  override Obj sqlToFan(CCol col, Obj sql)
  {
    switch (col.type.toNonNullable)
    {
      case Date#:
        Int v := sql
        y := v.shiftr(16).and(0xffff)
        m := v.shiftr(8).and(0xff)
        d := v.and(0xff)
        return Date(y, Month.vals[m-1], d)

      case DateTime#:
        return DateTime.makeTicks(sql, TimeZone.utc)

      default: return sql
    }
  }
}