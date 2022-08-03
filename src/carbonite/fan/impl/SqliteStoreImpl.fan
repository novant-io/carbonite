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
      case Str#:  sql.join("text",    " ")
      case Int#:  sql.join("integer", " ")
      case Date#: sql.join("integer", " ")
      //case DateTime#: sql.join("integer", " ")
      default:        throw ArgErr("Unsupported col type '${col.type}'")
    }

    // nullable
    if (!col.type.isNullable) sql.join("not null", " ")

    return sql.toStr
  }

}