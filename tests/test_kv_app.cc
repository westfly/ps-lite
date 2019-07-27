#include <cmath>
#include "ps/ps.h"
#include <algorithm> // for copy
#include <iterator> // for ostream_iterator
using namespace ps;

void StartServer() {
  if (!IsServer()) {
    return;
  }
  auto server = new KVServer<float>(0);
  server->set_request_handle(KVServerDefaultHandle<float>());
  RegisterExitCallback([server](){ delete server; });
}

void RunWorker() {
  if (!IsWorker()) return;
  KVWorker<float> kv(0, 0);

  // init
  int num = 10000;
  std::vector<Key> keys(num);
  std::vector<float> vals(num);

  int rank = MyRank();
  srand(rank + 7);
  for (int i = 0; i < num; ++i) {
    keys[i] = kMaxKey / num * i + rank;
    vals[i] = (rand() % 1000);
  }

  // push
  int repeat = 10;
  std::vector<int> ts;
  for (int i = 0; i < repeat; ++i) {
    ts.push_back(kv.Push(keys, vals));

    // to avoid too frequency push, which leads huge memory usage
    if (i > 10) kv.Wait(ts[ts.size()-10]);
  }
  for (int t : ts) kv.Wait(t);
  // https://stackoverflow.com/questions/10750057/how-to-print-out-the-contents-of-a-vector
  std::copy(ts.begin(), ts.end(), std::ostream_iterator<int>(std::cout, " "));
  std::cout<<std::endl << "copy vals\n";
  std::copy(vals.begin(), vals.end(), std::ostream_iterator<float>(std::cout, " "));
  std::cout<<std::endl;
  // pull
  std::vector<float> rets;
  kv.Wait(kv.Pull(keys, &rets));

  // pushpull
  std::vector<float> outs;
  for (int i = 0; i < repeat; ++i) {
    // PushPull on the same keys should be called serially
    kv.Wait(kv.PushPull(keys, vals, &outs));
  }

  float res = 0;
  float res2 = 0;
  for (int i = 0; i < num; ++i) {
    res += std::fabs(rets[i] - vals[i] * repeat);
    res2 += std::fabs(outs[i] - vals[i] * 2 * repeat);
  }
  for (size_t i = 0; i < rets.size(); ++i) {
	if (i % 1000 == 0) {
 		std::cout << MyRank() << " rets[" << i << "]: " << rets[i] << std::endl;
	}
  }
  CHECK_LT(res / repeat, 1e-5);
  CHECK_LT(res2 / (2 * repeat), 1e-5);
  LL << "error: " << res / repeat << ", " << res2 / (2 * repeat);
}

int main(int argc, char *argv[]) {
  // start system
  Start(0);
  // setup server nodes
  StartServer();
  // run worker nodes
  RunWorker();
  // stop system
  Finalize(0, true);
  return 0;
}
