//
// Copyright (c) 2022, Andy Frank
// Licensed under the MIT License
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
  new make(Str name, Type type, Str:Obj meta)
  {
    this.name = name
    this.type = type
    this.meta = meta
    this.primaryKey    = meta["primary_key"] == true
    this.autoIncrement = meta["auto_increment"] == true
    this.scopedBy      = meta["scoped_by"]
    this.defVal        = meta["def_val"]

  }

  ** Name of this column.
  const Str name

  ** Fantom type of this column.
  const Type type

  ** Metadata for this column.
  const Str:Obj meta

  ** Is this column a primary key?
  const Bool primaryKey

  ** Does this column have auto increment meta?
  const Bool autoIncrement

  ** Column this instance is scoped by or 'null' for none.
  const Str? scopedBy

  ** Default for this value or 'null' if undefined.
  const Obj? defVal

  ** Does this column require a non-null value on create.
  Bool req()
  {
    if (primaryKey && autoIncrement) return false
    if (defVal != null) return false
    return !type.isNullable
  }

  override Int compare(Obj that) { name.compare(that) }

  override Int hash() { name.hash }

  override Str toStr() { name }
}