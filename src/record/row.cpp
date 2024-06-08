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
  //计算位图所需要的字节数
  uint32_t bitmap_size = (fields_.size() + 7) / 8;
  char *bitmap = new char[bitmap_size];
  std::memset(bitmap, 0, bitmap_size);
  //初始化位图
  for (uint32_t i = 0; i < fields_.size(); i++) {
    if (fields_[i]->IsNull()) {
      bitmap[i / 8] |= (1 << (i % 8));
    } else {
      bitmap[i / 8] &= ~(1 << (i % 8));
    }
  }
  //序列化位图大小
  MACH_WRITE_UINT32(buf, bitmap_size);
  buf += sizeof(uint32_t);
  //序列化位图
  memcpy(buf, bitmap, bitmap_size);
  buf += bitmap_size;
  //序列化每个field
  for (auto field : fields_) {
    buf += field->SerializeTo(buf);
  }
  return buf - p;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(fields_.empty(), "Non empty field in row.");
  // replace with your code here
  //return 0;
  char *p = buf;
  //获取位图大小
  uint32_t bitmap_size = MACH_READ_UINT32(buf);
  buf += sizeof(uint32_t);
  //获取位图
  std::string bitmap(buf, bitmap_size);
  buf += bitmap_size;
  //获取schema的column
  std::vector<Column *> columns = schema->GetColumns();
  //将field的大小和column的大小统一
  fields_.resize(schema->GetColumnCount());
  //field反序列化
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    buf += Field::DeserializeFrom(buf, columns[i]->GetType(), &fields_[i], (bitmap[i / 8] >> (i % 8)) & 1);
  }
  return buf - p;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  // replace with your code here
  //return 0;
  uint32_t size = sizeof(uint32_t);
  size += (fields_.size() + 7) / 8;
  for (auto field : fields_) {
    size += field->GetSerializedSize();
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
