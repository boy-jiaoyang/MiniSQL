#include "record/schema.h"

/**
 * TODO: Student Implement
 */
uint32_t Schema::SerializeTo(char *buf) const {
  // replace with your code here
  //return 0;
  ASSERT(GetSerializedSize() <= PAGE_SIZE, "Fail to serialze schema to disk");
  char *p = buf;
  // Write the magic number to the buffer
  MACH_WRITE_UINT32(buf, SCHEMA_MAGIC_NUM);
  buf += sizeof(uint32_t);
  // Write the number of columns to the buffer
  MACH_WRITE_UINT32(buf, columns_.size());
  buf += sizeof(uint32_t);
  // Write each column to the buffer
  for(auto column : columns_) {
    buf += column->SerializeTo(buf);
  }
  return buf - p;
}

uint32_t Schema::GetSerializedSize() const {
  // replace with your code here
  //return 0;
  uint32_t size = sizeof(uint32_t) * 2;
  for(auto column : columns_) {
    size += column->GetSerializedSize();
  }
  return size;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
  // replace with your code here
  //return 0;
  ASSERT(schema == nullptr, "Fail to deserialize schema from disk");
  char *p = buf;
  // Read the magic number from the buffer
  uint32_t magic_num;
  MACH_WRITE_UINT32(buf, magic_num);
  buf += sizeof(uint32_t);
  ASSERT(magic_num == SCHEMA_MAGIC_NUM, "Failed to deserialize schema");
  // Read the number of columns from the buffer
  uint32_t num_columns = MACH_READ_UINT32(buf);
  buf += sizeof(uint32_t);
  // Read each column from the buffer
  std::vector<Column *> columns;
  for(uint32_t i = 0; i < num_columns; i++) {
    Column *column = nullptr;
    buf += Column::DeserializeFrom(buf, column);
    columns.push_back(column);
  }
  bool is_manage_ = MACH_READ_UINT32(buf) != 0;
  schema = new Schema(columns, is_manage_);
  return buf - p;
}