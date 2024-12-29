//
// Copyright (c) 2022, Andy Frank
// Licensed under the MIT License
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
  Int[] createAll([Str:Obj?][] batch)
  {
    // find union of all column names
    cols := Str:Str[:]
    batch.each |r| {
      r.each |v,k| { cols[k] = k }
    }

    // batch create
    return store.impl.createAll(this, cols.keys, batch)
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

  ** List records by given ids.
  CRec[] listIds(Int[] ids)
  {
    store.impl.selectIds(this, ids)
  }

  ** List records in this table where all 'where' conditions are equal.
  CRec[] listBy(Str:Obj where)
  {
    store.impl.select(this, "*", where)
  }

  ** List records joined between this table and given 'join' table where
  ** all join 'on' conditions are equal.
  CRec[] listJoin(Obj join, Str joinCol, [Str:Obj]? where := null)
  {
    joinTable := store.table(join)
    return store.impl.selectJoin(this, joinTable, joinCol, where)
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

  ** Update a list of existing records where 'batch' is map of existing
  ** record id to fields to update.  This method differs from `updateAll`
  ** in that each row may contain a different field value to update. All
  ** entries are required to have the set of field names to update.
  Void updateBatch(Int:[Str:Obj?] batch)
  {
    // short-circuit if nothing todo
    if (batch.isEmpty) return

    // optimize if a single op
    if (batch.size == 1)
    {
      id := batch.keys.first
      return update(id, batch[id])
    }

    // require all keys to match and contain a nullable value
    // to avoid inadvertently deleting if field is "blank"
    cols := Str:Str[:]
    batch.vals.first.each |v,k| { cols.add(k, k) }
    batch.vals.each |f| {
      cols.each |c| {
        if (!f.containsKey(c))
          throw ArgErr("Missing nullable field value for '${c}'")
      }
    }

    store.impl.updateBatch(this, cols.keys, batch)
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
