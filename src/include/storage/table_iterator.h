#ifndef MINISQL_TABLE_ITERATOR_H
#define MINISQL_TABLE_ITERATOR_H

#include "common/rowid.h"
#include "concurrency/txn.h"
#include "record/row.h"

class TableHeap;

class TableIterator {
public:
 // you may define your own constructor based on your member variables
 explicit TableIterator(TableHeap *table_heap, RowId rid, Txn *txn, bool is_begin = false,
                        bool is_end = false);

  // copy constructor
  // you may define your own copy constructor based on your member variables
  // TableIterator(const TableHeap *table_heap, const RowId &rid, const Txn *txn, bool is_begin = false,
  //               bool is_end = false);

  // copy constructor
  // you may define your own copy constructor based on your member variables

  TableIterator(const TableIterator &other);

  virtual ~TableIterator();

  bool operator==(const TableIterator &itr) const;

  bool operator!=(const TableIterator &itr) const;

  const Row &operator*();

  Row *operator->();

  TableIterator &operator=(const TableIterator &itr) noexcept;

  TableIterator &operator++();

  TableIterator operator++(int);

private:
  TableHeap *table_heap_;
  RowId rid_;
  Txn *txn_;
  bool is_end_{false};  // true if this iterator is end iterator
  bool is_begin_{false};  // true if this iterator is begin iterator
};

#endif  // MINISQL_TABLE_ITERATOR_H
