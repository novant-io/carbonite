//
// Copyright (c) 2022, Andy Frank
// Licensed under the MIT License
//
// History:
//   3 Mar 2024  Andy Frank  Creation
//

*************************************************************************
** CUtil
*************************************************************************

internal const class CUtil
{
  **
  ** Create insert sql string:
  **
  **   insert into xxx (a,b,c) values(?,?,?)
  **
  static Str sqlInsert(CTable table, CCol[] cols)
  {
    buf := StrBuf()
    buf.add("insert into ").add(table.name).add(" (")
    cols.each |c,i|
    {
      if (i > 0) buf.addChar(',')
      buf.addChar('"').add(c.name).addChar('"')
    }
    buf.add(") values(")
    cols.each |c,i|
    {
      if (i > 0) buf.addChar(',')
      buf.addChar('?')
    }
    buf.addChar(')')
    return buf.toStr
  }

  **
  ** Create insert sql string:
  **
  **   update xxx set (a=?,b=?,c=?) where id = x
  **
  static Str sqlUpdate(CTable table, CCol[] cols)
  {
    buf := StrBuf()
    buf.add("update ").add(table.name).add(" set ")
    cols.each |c,i|
    {
      if (i > 0) buf.addChar(',')
      buf.addChar('"').add(c.name).add("\"=?")
    }
    buf.add(" where id = ?")
    return buf.toStr
  }

  **
  ** Create delete sql string:
  **
  **   delete from xxx where a = ? and b = ?
  **
  static Str sqlDelete(CTable table, Str[] cols)
  {
    buf := StrBuf()
    buf.add("delete from ").add(table.name).add(" where")
    cols.each |c,i| {
      if (i > 0) buf.add(" and")
      buf.add(" \"").add(c).add("\" = ?")
    }
    return buf.toStr
  }

  ** Batch the given list into chunks of size 'chunkSize' and
  ** invoke the callback function for each chunk.
  static Void batch(Obj[] list, Int chunkSize, |Obj[] chunk| f)
  {
    off := 0
    while (off < list.size)
    {
      // get next chunk
      start := off
      end   := list.size.min(start+chunkSize)
      chunk := list[start..<end]
      f(chunk)

      // advance offset
      off = end
    }
  }

  ** Build column select from opts.
  static Str selectCols(CTable table, Str:Obj? opts)
  {
    // short-circuit if no options
    if (opts.isEmpty) return "*"

    // include specific cols
    inc := opts["include"] as Str[]
    if (inc != null) return inc.join(",") |c| { "\"${c}\"" }

    // exclude specific cols
    exc := opts["exclude"] as Str[]
    if (exc != null)
    {
      return table.cols
        .findAll |c| { !exc.contains(c.name) }
        .join(",") |c| { "\"${c.name}\"" }
    }

    return "*"
  }
}