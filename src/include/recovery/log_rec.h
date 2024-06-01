//
// Created by 26912 on 2024/5/30.
//

#ifndef LOG_REC_H
#define LOG_REC_H

#include <unordered_map>
#include <utility>

#include "common/config.h"
//#include "common/rowid.h"
#include "record/row.h"

using namespace std;

enum class log_rec{
  //日志保存类型
  tInvalid,
  tInsert,
  tDelete,
  tUpdate,
  tBegin,
  tCommit,
  tAbort,
};

using KeyType = std::string;
using ValType = int32_t;

/*note here 类型命名
using txn_id_t = int32_t;
using lsn_t = int32_t;
*/

struct LogRec{
  LogRec() = default;
  LogRec(log_rec type, lsn_t now_lsn, txn_id_t txn_id, lsn_t pre_lsn)
    : type_(type), lsn_(now_lsn), txn_id_(txn_id), pre_lsn_(pre_lsn){}

  log_rec type_{log_rec::tInvalid};
  lsn_t lsn_{INVALID_LSN};
  txn_id_t txn_id_{INVALID_TXN_ID};
  lsn_t pre_lsn_{INVALID_LSN};
  //insert
  KeyType ins_key_{};
  ValType ins_val_{};
  //delete
  KeyType del_key_{};
  ValType del_val_{};
  //update
  KeyType o_key_{};
  ValType o_val_{};
  KeyType n_key_{};
  ValType n_val_{};

  static unordered_map<txn_id_t, lsn_t> pre_lsn_map_;
  static lsn_t next_lsn_;

  static lsn_t GetUpdatePreLSN(txn_id_t txn_id, lsn_t lsn) {
    auto iter = pre_lsn_map_.find(txn_id);
    auto pre_lsn = INVALID_LSN;
    //get PreLSN
    if (iter != pre_lsn_map_.end()) {
      //找到,update
      pre_lsn = iter->second;
      iter->second = lsn;
    } else {
      //没找到，start，添加起始状态
      pre_lsn_map_.emplace(txn_id, lsn);
    }
    return pre_lsn;
  }

  //init
//std::unordered_map<txn_id_t, lsn_t> LogRec::prev_lsn_map_ = {};
//lsn_t LogRec::next_lsn_ = 0;

  typedef shared_ptr<LogRec> LogRecPtr;

static LogRecPtr CreateInsertLog(txn_id_t txn_id, KeyType ins_key, ValType ins_val) {
  lsn_t lsnn = LogRec::next_lsn_ ++;
  auto log = make_shared<LogRec>(log_rec::tInsert, lsnn, txn_id, LogRec::GetUpdatePreLSN(txn_id, lsnn));
  log->ins_key_ = move(ins_key);
  log->ins_val_ = ins_val;
  return log;
}

static LogRecPtr CreateDeleteLog(txn_id_t txn_id, KeyType del_key, ValType del_val) {
  lsn_t lsnn = LogRec::next_lsn_++;
  auto log = make_shared<LogRec>(log_rec::tDelete, lsnn, txn_id, LogRec::GetUpdatePreLSN(txn_id, lsnn));
  log->del_key_ = move(del_key);
  log->del_val_ = del_val;
  return log;
}

static LogRecPtr CreateUpdateLog(txn_id_t txn_id, KeyType o_key, ValType o_val, KeyType n_key, ValType n_val) {
  lsn_t lsnn = LogRec::next_lsn_++;
  auto log = make_shared<LogRec>(log_rec::tUpdate, lsnn, txn_id, LogRec::GetUpdatePreLSN(txn_id, lsnn));
  log->o_key_ = move(o_key);
  log->o_val_ = o_val;
  log->n_key_ = move(n_key);
  log->n_val_ = n_val;
  return log;
}

static LogRecPtr CreateBeginLog(txn_id_t txn_id) {
  lsn_t lsnn = LogRec::next_lsn_++;
  auto log = make_shared<LogRec>(log_rec::tBegin, lsnn, txn_id, LogRec::GetUpdatePreLSN(txn_id, lsnn));
  return log;
}

static LogRecPtr CreateCommitLog(txn_id_t txn_id) {
  lsn_t lsnn = LogRec::next_lsn_++;
  auto log = make_shared<LogRec>(log_rec::tCommit, lsnn, txn_id, LogRec::GetUpdatePreLSN(txn_id, lsnn));
  return log;
}

static LogRecPtr CreateAbortLog(txn_id_t txn_id) {
  lsn_t lsnn = LogRec::next_lsn_++;
  auto log = make_shared<LogRec>(log_rec::tAbort, lsnn, txn_id, LogRec::GetUpdatePreLSN(txn_id, lsnn));
  return log;
}

};
#endif //LOG_REC_H
