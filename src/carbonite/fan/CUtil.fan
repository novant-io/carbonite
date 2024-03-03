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
  static Str sqlInsert(CTable table, Str[] cols)
  {
    buf := StrBuf()
    buf.add("insert into ").add(table.name).add(" (")
    cols.each |c,i|
    {
      if (i > 0) buf.addChar(',')
      buf.add(c)
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
}