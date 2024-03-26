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

  ** Table constraints for this table.
  virtual CConstraint[] constraints() { CConstraint#.emptyList }

  ** Str representation of this table.
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
  ** return the new record id.
  Int create(Str:Obj? fields)
  {
    store.impl.create(this, fields)
  }

  ** Batch create a list of new records in this table with given
  ** list of new field values.  Returns list of new record ids.
  Int[] createAll([Str:Obj?][] rows)
  {
    // find union of all column names
    cols := Str:Str[:]
    rows.each |r| {
      r.each |v,k| { cols[k] = k }
    }

    // batch create
    return store.impl.createAll(this, cols.keys, rows)
  }

  ** Get record by id.
  CRec? get(Int id)
  {
    // TODO: check if cols.hasid ?
    store.impl.select(this, "*", ["id":id]).first
  }

  ** Get first record where all 'where' conditions are met.
  CRec? getBy(Str:Obj where)
  {
    store.impl.select(this, "*", where).first
  }

  ** List records in this table.
  CRec[] listAll()
  {
    store.impl.select(this, "*")
  }

  ** List records in this table where all 'where' conditions are equal.
  CRec[] listBy(Str:Obj where)
  {
    store.impl.select(this, "*", where)
  }

  ** Update existing record in this table with given field values.
  Void /*CRec*/ update(Int id, Str:Obj? fields)
  {
    store.impl.update(this, id, fields)
  }

  ** Update a list of existing record with given ids with given field values.
  Void updateAll(Int[] ids, Str:Obj? fields)
  {
    store.impl.updateAll(this, ids, fields)
  }

  ** Delete an existing record in this given id.
  Void delete(Int id)
  {
    store.impl.delete(this, id)
  }

  ** Delete the list of existing records with given ids.
  Void deleteAll(Int[] ids)
  {
    store.impl.deleteAll(this, ids)
  }

  ** Delete all records where all 'where' conditions are equal.
  Void deleteBy(Str:Obj where)
  {
    store.impl.deleteBy(this, where)
  }

  internal Void init(CStore store)
  {
    this.storeRef.val = store
    this.cmapRef.val  = Str:CCol[:].setList(cols) |c| { c.name }.toImmutable
    this.scopedIdRef.val = cols.any |c| { c.scopedBy != null }
  }

  private CStore store() { storeRef.val }
  private const AtomicRef storeRef := AtomicRef(null)

  internal Str:CCol cmap() { cmapRef.val }
  private const AtomicRef cmapRef := AtomicRef(null)

  internal Bool hasScopedId() { scopedIdRef.val }
  private const AtomicBool scopedIdRef := AtomicBool(false)
}
