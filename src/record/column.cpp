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
  // replace with your code here
  //return 0;
  ASSERT(GetSerializedSize() <= PAGE_SIZE, "Fail to serialze colum to disk");
  char *p = buf;
  // Write the magic number to the buffer
  MACH_WRITE_UINT32(buf, COLUMN_MAGIC_NUM);
  buf += sizeof(uint32_t);
  // Get the length of the column name and write it to the buffer
  uint32_t column_name_length = name_.length();
  MACH_WRITE_UINT32(buf, column_name_length);
  buf += sizeof(uint32_t);
  // Write the column name string to the buffer
  MACH_WRITE_STRING(buf, name_);
  buf += column_name_length;
  // Write the type, length, column index, nullable flag, and unique flag to the buffer
  MACH_WRITE_UINT32(buf, static_cast<uint32_t>(type_));
  buf += sizeof(uint32_t);
  MACH_WRITE_UINT32(buf, len_);
  buf += sizeof(uint32_t);
  MACH_WRITE_UINT32(buf, table_ind_);
  buf += sizeof(uint32_t);
  MACH_WRITE_UINT32(buf, nullable_);
  buf += sizeof(uint32_t);
  MACH_WRITE_UINT32(buf, unique_);
  buf += sizeof(uint32_t);
  return buf - p;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::GetSerializedSize() const {
  // replace with your code here
  //return 0;
  uint32_t size = sizeof(uint32_t) * 7 + name_.length();
  return size;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
  // replace with your code here
  //return 0;
  ASSERT(column == nullptr, "Fail to deserialize column from disk");
  // Allocate memory for the column object
  char *p = buf;
  void *mem = malloc(sizeof(Column));
  // Read the magic number from the buffer
  uint32_t magic_num = MACH_READ_UINT32(buf);
  buf += sizeof(uint32_t);
  // Check if the magic number is correct
  ASSERT(magic_num == COLUMN_MAGIC_NUM, "Fail to deserialize column from disk");
  // Read the length of the column name from the buffer
  uint32_t column_name_length = MACH_READ_UINT32(buf);
  buf += sizeof(uint32_t);
  // Read the column name string from the buffer
  std::string column_name = std::string(reinterpret_cast<const char *> (buf), (column_name_length));
  buf += column_name_length;
  // Read the type, length, column index, nullable flag, and unique flag from the buffer
  TypeId type = static_cast<TypeId>(MACH_READ_UINT32(buf));
  buf += sizeof(uint32_t);
  uint32_t len = MACH_READ_UINT32(buf);
  buf += sizeof(uint32_t);
  uint32_t table_ind = MACH_READ_UINT32(buf);
  buf += sizeof(uint32_t);
  bool nullable = MACH_READ_UINT32(buf);
  buf += sizeof(uint32_t);
  bool unique = MACH_READ_UINT32(buf);
  buf += sizeof(uint32_t);
  // Create a new column object
  if(type == TypeId::kTypeChar){
    column = new (mem) Column(column_name, type, len, table_ind, nullable, unique);
  }else {
    column = new (mem) Column(column_name, type, table_ind, nullable, unique);
  }
  return buf - p;
}
