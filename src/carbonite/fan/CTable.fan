//
// Copyright (c) 2022, Novant LLC
// All Rights Reserved
//
// History:
//   4 Jul 2022  Andy Frank  Creation
//

using concurrent

*************************************************************************
** CTable
*************************************************************************

** CTable models a database table for a `CStore`.
abstract const class CTable
{
  ** Name of this table.
  abstract Str name()

  ** Column names for this table.
  abstract CCol[] cols()

  override Str toStr()
  {
    "table ${name} (" + (cols.join(", ") |c| { c.name }) + ")"
  }

  ** Return number of records in this table.
  Int size()
  {
    store.impl.tableSize(this)
  }

  ** Create a new record in this table with given field values and
  ** return the new record instance.
  Void /*CRec*/ create(Str:Obj fields)
  {
    // verify field cols
    fields.each |v,k|
    {
      c := cmap[k]
      if (c == null) throw ArgErr("Field not a column: '${k}'")

      // TODO: check nullable
    }
    store.impl.create(this.name, fields)
  }

  ** Get record by id.
  CRec? get(Int id)
  {
    // TODO: check if cols.hasid ?
    store.impl.select(this.name, "*", ["id":id]).first
  }

  ** List records in this table.
  CRec[] listAll()
  {
    store.impl.select(this.name, "*")
  }

  ** Update existing record in this table with given field values and
  ** return the new record instance.
  Void /*CRec*/ update(Int id, Str:Obj fields)
  {
    // verify field cols
    fields.each |v,k|
    {
      c := cmap[k]
      if (c == null) throw ArgErr("Field not a column: '${k}'")

      // TODO: check nullable
    }
    store.impl.update(this.name, id, fields)
  }

  internal Void init(CStore store)
  {
    this.storeRef.val = store
    this.cmapRef.val  = Str:CCol[:].setList(cols) |c| { c.name }.toImmutable
  }

  private CStore store() { storeRef.val }
  private const AtomicRef storeRef := AtomicRef(null)

  private Str:CCol cmap() { cmapRef.val }
  private const AtomicRef cmapRef := AtomicRef(null)
}
