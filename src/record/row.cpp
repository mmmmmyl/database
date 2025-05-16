#include "record/row.h"

/**
 * TODO: Student Implement
 */
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  // replace with your code here
  uint32_t offset = 0;
  uint32_t header_size = schema->GetSerializedSize();
  memcpy(buf, &header_size, sizeof(uint32_t));
  offset += sizeof(uint32_t);
  memcpy(buf + offset, &rid_, sizeof(RowId));
  offset += sizeof(RowId);
  uint32_t bitmap = 0;
  for (uint32_t i = 0; i < fields_.size(); i++) {
    if (fields_[i]->IsNull()) bitmap |= (1 << i);
    else bitmap &= ~(1 << i);
  }
  memcpy(buf + offset, &bitmap, sizeof(uint32_t));
  offset += sizeof(uint32_t);
  for (uint32_t i = 0; i < fields_.size(); i++) {
    if (fields_[i]->IsNull()) {
      continue;
    }
    offset += fields_[i]->SerializeTo(buf + offset);
  }
  return offset;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(fields_.empty(), "Non empty field in row.");
  // replace with your code here
  uint32_t offset = 0;
  uint32_t header_size;
  memcpy(&header_size, buf + offset, sizeof(uint32_t));
  offset += sizeof(uint32_t);
  memcpy(&rid_, buf + offset, sizeof(RowId));
  offset += sizeof(RowId);
  uint32_t bitmap;
  memcpy(&bitmap, buf + offset, sizeof(uint32_t));
  offset += sizeof(uint32_t);
  fields_.resize(schema->GetColumnCount());
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    fields_[i] = new Field(schema->GetColumn(i)->GetType());
    fields_[i]->DeserializeFrom(buf + offset, schema->GetColumn(i)->GetType(), &fields_[i], bitmap & (1 << i));
    offset += fields_[i]->GetSerializedSize();
  }
  return offset;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  // replace with your code here
  uint32_t offset = 0;
  offset += sizeof(uint32_t);
  offset += sizeof(RowId);
  offset += sizeof(uint32_t);
  for(auto field : fields_) {
    if (field->IsNull()) {
      continue;
    }
    offset += field->GetSerializedSize();
  }
  return offset;
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  for (auto column : columns) {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);
}
