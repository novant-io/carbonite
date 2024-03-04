//
// Copyright (c) 2024, Novant LLC
// All Rights Reserved
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
}