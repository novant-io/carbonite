//
// Copyright (c) 2022, Novant LLC
// All Rights Reserved
//
// History:
//   3 Aug 2022  Andy Frank  Creation
//

using carbonite

*************************************************************************
** Types
*************************************************************************

const class Types : CTable
{
  override const Str name := "types"
  override const CCol[] cols := [
    CCol("str",  Str#,  [:]),
    CCol("int",  Int#,  [:]),
    // CCol("date", Date#, [:]),
  ]
}