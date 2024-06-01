//
// Created by 26912 on 2024/5/31.
//

#ifndef MINISQL_RECOVERY_MANAGER_H
#define MINISQL_RECOVERY_MANAGER_H

#include <map>
#include <unordered_map>
#include <vector>

#include "recovery/log_rec.h"

using KvDatabase = std::unordered_map<KeyType, ValType>;
using ATT = std::unordered_map<txn_id_t, lsn_t>;
using LogRecPtr = std::shared_ptr<LogRec>;

struct CheckPoint {
  lsn_t checkpoint_lsn_{INVALID_LSN};
  ATT active_txns_{};
  KvDatabase persist_data_{};

  inline void AddActiveTxn(txn_id_t txn_id, lsn_t last_lsn) { active_txns_[txn_id] = last_lsn; }

  inline void AddData(KeyType key, ValType val) { persist_data_.emplace(std::move(key), val); }
};

class RecoveryManager {
public:
  /*
     初始化恢复管理器，基于上一个检查点
     @param last_checkpoint 最后一个检查点
     */
  void Init(CheckPoint &last_checkpoint) {
    // 复制上一个检查点的LSN
    persist_lsn_ = last_checkpoint.checkpoint_lsn_;
    // 复制活动事务表
    active_txns_ = last_checkpoint.active_txns_;
    // 复制持久化数据
    data_ = last_checkpoint.persist_data_;
  }

  //Redo 阶段：重新执行所有已提交的事务
  void RedoPhase() {
    for (const auto &entry : log_recs_) {
      const auto &log = entry.second;
      if (log->lsn_ > persist_lsn_) {
        switch (log->type_) {
          case log_rec::tInsert:
            data_[log->ins_key_] = log->ins_val_;
          break;
          case log_rec::tDelete:
            data_.erase(log->del_key_);
          break;
          case log_rec::tUpdate:
            data_[log->n_key_] = log->n_val_;
          break;
          default:
            break;
        }
      }
    }
  }


  //Undo 阶段：撤销所有未提交的事务
  void UndoPhase() {
    for (auto it = log_recs_.rbegin(); it != log_recs_.rend(); ++it) {
      const auto &log = it->second;
      if (active_txns_.find(log->txn_id_) != active_txns_.end()) {
        switch (log->type_) {
          case log_rec::tInsert:
            data_.erase(log->ins_key_);
          break;
          case log_rec::tDelete:
            data_[log->del_key_] = log->del_val_;
          break;
          case log_rec::tUpdate:
            data_[log->o_key_] = log->o_val_;
          break;
          default:
            break;
        }
      }
    }
  }

  // used for test only
  void AppendLogRec(LogRecPtr log_rec) { log_recs_.emplace(log_rec->lsn_, log_rec); }

  // used for test only
  inline KvDatabase &GetDatabase() { return data_; }

private:
  std::map<lsn_t, LogRecPtr> log_recs_{};
  lsn_t persist_lsn_{INVALID_LSN};
  ATT active_txns_{};
  KvDatabase data_{};  // all data in database
};

#endif  // MINISQL_RECOVERY_MANAGER_H
