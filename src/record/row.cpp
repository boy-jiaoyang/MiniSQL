#include "record/row.h"

#include <page/bitmap_page.h>

/**
 * TODO: Student Implement
 */
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  // replace with your code here
  //return 0;
  char *p = buf;
  // Write the row id to the buffer
  RowId rid = GetRowId();
  MACH_WRITE_UINT32(buf, rid.GetPageId());
  buf += sizeof(uint32_t);
  MACH_WRITE_UINT32(buf, rid.GetSlotNum());
  buf += sizeof(uint32_t);
  // 记录空字段的位置
  std::vector<bool> nulls;
  for (auto field : fields_) {
    nulls.push_back(field->IsNull());
  }
  for (auto null : nulls) {
    MACH_WRITE_UINT8(buf, null);
    buf += sizeof(uint8_t);
  }
  // Write each field to the buffer
  for (uint32_t i = 0; i < fields_.size(); i++) {
    if (!nulls[i]) {
      buf += fields_[i]->SerializeTo(buf);
    }
  }
  return buf - p;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(fields_.empty(), "Non empty field in row.");
  // replace with your code here
  //return 0;
  char *p = buf;
  // Read the row id from the buffer
  uint32_t page_id = MACH_READ_UINT32(buf);
  buf += sizeof(uint32_t);
  uint32_t slot_num = MACH_READ_UINT32(buf);
  buf += sizeof(uint32_t);
  SetRowId(RowId(page_id, slot_num));
  // Read the null fields from the buffer
  std::vector<bool> nulls;
  for(uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    bool null = MACH_READ_UINT8(buf) != 0;
    buf += sizeof(uint8_t);
    nulls.push_back(null);
  }
  // Read each field from the buffer
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    Field *field = nullptr;
    buf += Field::DeserializeFrom(buf, schema->GetColumn(i)->GetType(), &field, nulls[i]);
    fields_.push_back(field);
  }
  return buf - p;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  // replace with your code here
  //return 0;
  uint32_t size = sizeof(uint32_t) * 2;
  size += fields_.size();
  for (uint32_t i = 0; i < fields_.size(); i++) {
    if (!fields_[i]->IsNull()) {
      size += fields_[i]->GetSerializedSize();
    }
  }
  return size;
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
