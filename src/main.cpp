#include <cstdio>
#include <iomanip>

#include "executor/execute_engine.h"
#include "glog/logging.h"
#include "parser/syntax_tree_printer.h"
#include "utils/tree_file_mgr.h"

extern "C" {
int yyparse(void);
FILE *yyin;
#include "parser/minisql_lex.h"
#include "parser/parser.h"
}

void InitGoogleLog(char *argv) {
  FLAGS_logtostderr = true;
  FLAGS_colorlogtostderr = true;
  google::InitGoogleLogging(argv);
  // LOG(INFO) << "glog started!";
}

void InputCommand(char *input, const int len, ExecuteEngine* engine) {

  memset(input, 0, len);
  printf("minisql > ");
  int i = 0;
  char ch;

  while ((ch = getchar()) != ';') {
    // 针对于执行文件sql语句的情况，如果到了文件末尾，关闭文件，同时重定向回标准输入
    if(ch == EOF) {
      fclose(stdin);
      freopen("/dev/tty", "r", stdin);
      // 记录结束时间，同时输出文件花费的总时间
      auto file_stop_time = std::chrono::system_clock::now();
      auto file_start_time = engine->file_start_time;
      double duration_time = double((std::chrono::duration_cast<std::chrono::milliseconds>(file_stop_time-file_start_time).count()));
      cout << "End of execution file, total time(" << fixed << setprecision(4) << duration_time/1000 << "sec)." << std::endl;
      continue;
    }
    input[i++] = ch;
  }
  input[i] = ch;  // ;
  getchar();      // remove enter
}

int main(int argc, char **argv) {
  InitGoogleLog(argv[0]);
  // command buffer
  const int buf_size = 1024;
  char cmd[buf_size];
  // executor engine
  ExecuteEngine engine;
  // for print syntax tree
  TreeFileManagers syntax_tree_file_mgr("syntax_tree_");
  uint32_t syntax_tree_id = 0;

  while (1) {
    // read from buffer

    InputCommand(cmd, buf_size, &engine);
    cout << cmd << endl;
    // create buffer for sql input
    YY_BUFFER_STATE bp = yy_scan_string(cmd);
    if (bp == nullptr) {
      LOG(ERROR) << "Failed to create yy buffer state." << std::endl;
      exit(1);
    }
    yy_switch_to_buffer(bp);

    // init parser module
    MinisqlParserInit();

    // parse
    yyparse();

    // parse result handle
    if (MinisqlParserGetError()) {
      // error
      printf("%s\n", MinisqlParserGetErrorMessage());
    } else {
      // Comment them out if you don't need to debug the syntax tree
//      printf("[INFO] Sql syntax parse ok!\n");
//      SyntaxTreePrinter printer(MinisqlGetParserRootNode());
//      printer.PrintTree(syntax_tree_file_mgr[syntax_tree_id++]);
    }

    auto result = engine.Execute(MinisqlGetParserRootNode());
    // clean memory after parse
    MinisqlParserFinish();
    yy_delete_buffer(bp);
    yylex_destroy();

    // quit condition
    engine.ExecuteInformation(result);
    if (result == DB_QUIT) {
      break;
    }
  }
  return 0;
}