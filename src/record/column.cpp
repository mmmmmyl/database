#include "record/column.h"

#include "glog/logging.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

/**
* TODO: Student Implement
*/
uint32_t Column::SerializeTo(char *buf) const {
  MACH_WRITE_INT32(buf,COLUMN_MAGIC_NUM);
  buf += sizeof(uint32_t);
  MACH_WRITE_UINT32(buf,name_.length());
  MACH_WRITE_STRING(buf+sizeof(uint32_t),name_);
  buf += MACH_STR_SERIALIZED_SIZE(name_);
  MACH_WRITE_UINT32(buf,len_);
  buf += sizeof(uint32_t);
  MACH_WRITE_UINT32(buf,table_ind_);
  buf += sizeof(uint32_t);
  MACH_WRITE_UINT32(buf,nullable_);
  buf += sizeof(uint32_t);
  MACH_WRITE_UINT32(buf,unique_);
  buf += sizeof(uint32_t);
  MACH_WRITE_UINT32(buf,type_);
  buf += sizeof(uint32_t);
  return sizeof(uint32_t) * 6 + MACH_STR_SERIALIZED_SIZE(name_);
}

/**
 * TODO: Student Implement
 */
uint32_t Column::GetSerializedSize() const {
  return sizeof(uint32_t) * 6 + MACH_STR_SERIALIZED_SIZE(name_);
  // if (type_ == TypeId::kTypeChar){
  //   return sizeof(uint32_t) * 6 + MACH_STR_SERIALIZED_SIZE(name_);
  // }
  // return sizeof(uint32_t) * 6 + sizeof(uint32_t);
}

/**
 * TODO: Student Implement
 */
uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
  uint32_t magic_num = MACH_READ_UINT32(buf);
  ASSERT(magic_num==COLUMN_MAGIC_NUM, "Invalid column magic number.");
  buf += sizeof(uint32_t);
  uint32_t strlen = MACH_READ_UINT32(buf);
  buf += sizeof(uint32_t);
  std::string name(buf, strlen);
  buf += strlen;
  uint32_t len = MACH_READ_UINT32(buf);
  buf += sizeof(uint32_t);
  uint32_t table_ind = MACH_READ_UINT32(buf);
  buf += sizeof(uint32_t);
  uint32_t nullable = MACH_READ_UINT32(buf);
  buf += sizeof(uint32_t);
  uint32_t unique = MACH_READ_UINT32(buf);
  buf += sizeof(uint32_t);
  uint32_t type = MACH_READ_UINT32(buf);
  buf += sizeof(uint32_t);
  if (type == TypeId::kTypeChar) {
    column = new Column(name, TypeId::kTypeChar, len, table_ind, nullable, unique);
  } else {
    column = new Column(name, static_cast<TypeId>(type), table_ind, nullable, unique);
  }
  return sizeof(uint32_t) * 7 + strlen;
}
