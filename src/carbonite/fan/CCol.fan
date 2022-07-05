//
// Copyright (c) 2022, Novant LLC
// All Rights Reserved
//
// History:
//   4 Jul 2022  Andy Frank  Creation
//

*************************************************************************
** CCol
*************************************************************************

** CCol models a column for for a `CTable`.
const class CCol
{
  ** Ctor.
  new make(Str name, Type type, Str:Obj[] meta)
  {
    this.name = name
    this.type = type
    this.meta = meta
  }

  ** Name of this column.
  const Str name

  ** Fantom type of this column.
  const Type type

  ** Metadata for this column.
  const Str:Obj meta

  override Str toStr() { name }
}