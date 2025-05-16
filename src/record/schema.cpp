#include "record/schema.h"

/**
 * TODO: Student Implement
 */
uint32_t Schema::SerializeTo(char *buf) const {
  uint32_t offset = 0;
  MACH_WRITE_INT32(buf,SCHEMA_MAGIC_NUM);
  offset += sizeof(uint32_t);
  MACH_WRITE_UINT32(buf + offset, columns_.size());
  offset += sizeof(uint32_t);
  for (const auto &column : columns_) {
    offset += column->SerializeTo(buf + offset);
  }
  return offset;
}

uint32_t Schema::GetSerializedSize() const {
  uint32_t size = sizeof(uint32_t) * 2; // magic number and column count
  for (const auto &column : columns_) {
    size += column->GetSerializedSize();
  }
  return size;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
  uint32_t offset = 0;
  uint32_t magic_num = MACH_READ_UINT32(buf);
  ASSERT(magic_num == SCHEMA_MAGIC_NUM, "Invalid schema magic number.");
  offset += sizeof(uint32_t);
  uint32_t column_count = MACH_READ_UINT32(buf+offset);
  offset += sizeof(uint32_t);
  std::vector<Column *> columns;
  columns.reserve(column_count);
  for (uint32_t i = 0; i < column_count; ++i) {
    Column *column;
    offset += Column::DeserializeFrom(buf+offset, column);
    columns.push_back(column);
  }
  schema = new Schema(columns, false);
  return offset;
}