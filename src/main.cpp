// Shopping Sessions pipeline: exact implementation of rules.md, multithreaded C++20.
// Reads $DATA_DIR, writes $OUTPUT_DIR/{user_daily,campaign_daily}/*.parquet.
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <dirent.h>
#include <fcntl.h>
#include <immintrin.h>
#include <string>
#include <string_view>
#include <sys/mman.h>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <vector>

using std::string;
using std::string_view;
using std::vector;
typedef int64_t i64;
typedef uint64_t u64;
typedef uint32_t u32;

static const i64 DAY_MS = 86400000LL;
static const i64 WEEK_MS = 7 * DAY_MS;
static const i64 GAP_MS = 1800000LL;
static const i64 BAD_TS = INT64_MIN;

static int NT = 8;
static auto T0 = std::chrono::steady_clock::now();
static bool PROF = getenv("SS_PROF") != nullptr;
static void lap(const char* what) {
  if (!PROF) return;
  double ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - T0).count();
  fprintf(stderr, "%8.1f ms  %s\n", ms, what);
}

[[noreturn]] static void die(const char* m) {
  fprintf(stderr, "fatal: %s (%s)\n", m, strerror(errno));
  exit(1);
}

// ---------------------------------------------------------------- hashing
static inline u64 mix64(u64 x) {
  x ^= x >> 32; x *= 0xd6e8feb86659fd93ULL; x ^= x >> 32; x *= 0xd6e8feb86659fd93ULL; x ^= x >> 32;
  return x;
}
static inline u64 hash_bytes(const char* p, u32 n) {
  u64 h = 0x9E3779B97F4A7C15ULL ^ (u64(n) * 0xff51afd7ed558ccdULL);
  while (n >= 8) { u64 v; memcpy(&v, p, 8); h = (h ^ mix64(v)) * 0x9E3779B97F4A7C15ULL; p += 8; n -= 8; }
  if (n) { u64 v = 0; memcpy(&v, p, n); h = (h ^ mix64(v ^ (u64(n) << 56))) * 0x9E3779B97F4A7C15ULL; }
  return mix64(h);
}

// ---------------------------------------------------------------- parallel helper
template <class F> static void parallel_for(size_t n, F f) {
  std::atomic<size_t> next{0};
  int nt = (int)std::min<size_t>(NT, std::max<size_t>(n, 1));
  vector<std::thread> th;
  auto work = [&](int tid) { for (size_t i; (i = next.fetch_add(1)) < n;) f(i, tid); };
  for (int t = 1; t < nt; t++) th.emplace_back(work, t);
  work(0);
  for (auto& x : th) x.join();
}

// ---------------------------------------------------------------- files
struct MFile { const char* p = nullptr; size_t n = 0; };
static MFile map_file(const string& path) {
  MFile f;
  int fd = open(path.c_str(), O_RDONLY);
  if (fd < 0) die(path.c_str());
  struct stat st; fstat(fd, &st);
  f.n = st.st_size;
  if (f.n) {
    void* m = mmap(nullptr, f.n, PROT_READ, MAP_PRIVATE, fd, 0);
    if (m == MAP_FAILED) die("mmap");
    f.p = (const char*)m;
  }
  close(fd);
  return f;
}
static vector<string> list_csv(const string& dir) {
  vector<string> out;
  DIR* d = opendir(dir.c_str());
  if (!d) return out;
  while (dirent* e = readdir(d)) {
    string n = e->d_name;
    if (n.size() > 4 && n.compare(n.size() - 4, 4, ".csv") == 0) out.push_back(dir + "/" + n);
  }
  closedir(d);
  std::sort(out.begin(), out.end());
  return out;
}

// Work chunks: lines belong to the chunk holding their first byte; each file's first line (header) is skipped.
struct Chunk { const char* base; size_t fsize, b, e; };
static vector<Chunk> make_chunks(const vector<MFile>& files, size_t target) {
  vector<Chunk> cs;
  for (auto& f : files)
    for (size_t b = 0; b < f.n; b += target) cs.push_back({f.p, f.n, b, std::min(f.n, b + target)});
  return cs;
}
// Calls fn(line_begin, line_end) for every data line starting in the chunk.
template <class F> static inline void for_lines(const Chunk& c, F fn) {
  const char* base = c.base;
  size_t pos;
  if (c.b == 0) {
    const char* nl = (const char*)memchr(base, '\n', c.fsize);
    if (!nl) return;
    pos = nl - base + 1;
  } else {
    const char* nl = (const char*)memchr(base + c.b - 1, '\n', c.fsize - (c.b - 1));
    if (!nl) return;
    pos = nl - base + 1;
  }
  while (pos < c.e) {
    const char* s = base + pos;
    const char* nl = (const char*)memchr(s, '\n', c.fsize - pos);
    const char* end = nl ? nl : base + c.fsize;
    fn(s, end);
    pos = (end - base) + 1;
  }
}

// ---------------------------------------------------------------- CSV dialect
struct SV { const char* p; u32 n; };
// Splits [s,e) into exactly k fields per rules.md. Returns false if malformed.
static inline bool split_fields(const char* s, const char* e, SV* out, int k) {
  if (s == e) return false;
  int i = 0;
  const char* p = s;
  for (;;) {
    if (i >= k) return false;
    if (p < e && *p == '"') {
      const char* q = (const char*)memchr(p + 1, '"', e - (p + 1));
      if (!q) return false;
      if (memchr(p + 1, ',', q - (p + 1))) return false;  // every comma separates fields
      out[i++] = {p + 1, u32(q - (p + 1))};
      p = q + 1;
      if (p == e) break;
      if (*p != ',') return false;
      p++;
      if (p == e) { if (i >= k) return false; out[i++] = {p, 0}; break; }
    } else {
      const char* q = (const char*)memchr(p, ',', e - p);
      const char* fe = q ? q : e;
      if (memchr(p, '"', fe - p)) return false;
      out[i++] = {p, u32(fe - p)};
      if (!q) break;
      p = q + 1;
    }
  }
  return i == k;
}

// SIMD line scanner: for every data line starting in chunk c that splits into exactly K fields
// under the rules' dialect, calls fn(fields). Malformed lines are skipped. Equivalent to
// for_lines + split_fields, but finds ',', '"' and '\n' 64 bytes at a time.
template <int K, class F> static inline void scan_lines(const Chunk& c, F fn) {
  const char* base = c.base;
  const size_t n = c.fsize;
  size_t pos;
  {
    const char* nl = (const char*)memchr(base + (c.b ? c.b - 1 : 0), '\n', n - (c.b ? c.b - 1 : 0));
    if (!nl) return;
    pos = nl - base + 1;
  }
  if (pos >= c.e) return;
  const __m256i vc = _mm256_set1_epi8(','), vq = _mm256_set1_epi8('"'), vn = _mm256_set1_epi8('\n');
  alignas(32) char tail[64];
  SV f[K];
  int fi = 0;            // index of the current field
  size_t fs = pos;       // current field start
  size_t line = pos;     // current line start
  int st = 0;            // 0 = in field (unquoted or at start), 1 = inside quotes, 2 = just after closing quote, 3 = malformed
  size_t qclose = 0;
  for (size_t blk = pos;; blk += 64) {
    const char* p = base + blk;
    u64 mn, mq, mc;
    if (blk + 64 <= n) {
      __m256i a = _mm256_loadu_si256((const __m256i*)p), b = _mm256_loadu_si256((const __m256i*)(p + 32));
      mn = (u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, vn)) | ((u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, vn)) << 32);
      mq = (u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, vq)) | ((u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, vq)) << 32);
      mc = (u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, vc)) | ((u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, vc)) << 32);
    } else {
      if (blk > n) return;
      size_t r = n - blk;
      memset(tail, 0, 64);
      memcpy(tail, p, r);
      tail[r] = '\n';  // virtual line end at EOF (r < 64 here)
      __m256i a = _mm256_load_si256((const __m256i*)tail), b = _mm256_load_si256((const __m256i*)(tail + 32));
      mn = (u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, vn)) | ((u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, vn)) << 32);
      mq = (u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, vq)) | ((u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, vq)) << 32);
      mc = (u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, vc)) | ((u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, vc)) << 32);
      u64 keep = (r + 1 >= 64) ? ~0ULL : ((1ULL << (r + 1)) - 1);
      mn &= keep; mq &= keep; mc &= keep;
    }
    u64 ev = mn | mq | mc;
    while (ev) {
      int bit = __builtin_ctzll(ev);
      ev &= ev - 1;
      size_t at = blk + bit;
      u64 m = 1ULL << bit;
      if (mn & m) {
        // line end at `at`
        if (st == 0) {
          if (fi == K - 1) { f[fi] = {base + fs, u32(at - fs)}; fn(f); }
        } else if (st == 2) {
          if (fi == K - 1 && at == qclose + 1) { f[fi] = {base + fs + 1, u32(qclose - fs - 1)}; fn(f); }
        }
        line = at + 1;
        if (line >= c.e) return;
        fi = 0; fs = line; st = 0;
      } else if (st == 3) {
        continue;
      } else if (mq & m) {
        if (st == 0) { st = (at == fs) ? 1 : 3; }
        else if (st == 1) { st = 2; qclose = at; }
        else st = 3;  // quote right after a closing quote, or later in the field
      } else {  // comma
        if (st == 1) { st = 3; continue; }
        if (fi >= K - 1) { st = 3; continue; }
        if (st == 0) f[fi] = {base + fs, u32(at - fs)};
        else {  // st == 2
          if (at != qclose + 1) { st = 3; continue; }
          f[fi] = {base + fs + 1, u32(qclose - fs - 1)};
        }
        fi++; fs = at + 1; st = 0;
      }
    }
  }
}

// ---------------------------------------------------------------- timestamps
static inline bool dig(char c) { return c >= '0' && c <= '9'; }
static inline i64 days_from_civil(i64 y, unsigned m, unsigned d) {
  y -= m <= 2;
  const i64 era = (y >= 0 ? y : y - 399) / 400;
  const unsigned yoe = (unsigned)(y - era * 400);
  const unsigned doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
  const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
  return era * 146097 + (i64)doe - 719468;
}
static inline int d2(const char* p) { return (p[0] - '0') * 10 + (p[1] - '0'); }
// Parses YYYY-MM-DDTHH:MM:SS at p (19 chars); returns epoch ms or BAD_TS.
static inline u64 ld8(const char* p) { u64 v; memcpy(&v, p, 8); return v; }
// true if every byte selected by mask m is an ASCII digit
static inline bool digits8(u64 x, u64 m) {
  const u64 hi = 0xF0F0F0F0F0F0F0F0ULL & m, t30 = 0x3030303030303030ULL & m, six = 0x0606060606060606ULL & m;
  return ((x & hi) == t30) && (((x + six) & hi) == t30);
}
static inline i64 parse_dt(const char* p) {
  // "YYYY-MM-DDTHH:MM:SS": bytes 0-7 "YYYY-MM-", 8-15 "DDTHH:MM", 11-18 "HH:MM:SS"
  const u64 a = ld8(p), b = ld8(p + 8), c = ld8(p + 11);
  const u64 MA = 0x00FFFF00FFFFFFFFULL, MB = 0xFFFF00FFFF00FFFFULL, MC = 0xFFFF00FFFF00FFFFULL;
  if (!digits8(a, MA) || !digits8(b, MB) || !digits8(c, MC)) return BAD_TS;
  if ((a & ~MA) != (0x2D00002D00000000ULL) || (b & ~MB) != (0x00003A0000540000ULL) || (c & ~MC) != (0x00003A00003A0000ULL)) return BAD_TS;
  int y = d2(p) * 100 + d2(p + 2), mo = d2(p + 5), d = d2(p + 8), h = d2(p + 11), mi = d2(p + 14), sec = d2(p + 17);
  if (mo < 1 || mo > 12 || d < 1 || h > 23 || mi > 59 || sec > 59) return BAD_TS;
  static const int dim[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
  int md = dim[mo - 1];
  if (mo == 2 && ((y % 4 == 0 && y % 100 != 0) || y % 400 == 0)) md = 29;
  if (d > md) return BAD_TS;
  return (days_from_civil(y, mo, d) * 86400 + h * 3600 + mi * 60 + sec) * 1000;
}
// Any of F1/F2/F3. Returns BAD_TS when malformed or (F3) outside int64.
static inline i64 parse_ts(const char* p, u32 n) {
  if (n == 0) return BAD_TS;
  if (n == 20) {
    if (p[19] == 'Z') return parse_dt(p);
  } else if (n == 25) {
    char sg = p[19];
    if ((sg == '+' || sg == '-') && dig(p[20]) && dig(p[21]) && p[22] == ':' && dig(p[23]) && dig(p[24])) {
      int oh = d2(p + 20), om = d2(p + 23);
      if (oh > 23 || om > 59) return BAD_TS;
      i64 t = parse_dt(p);
      if (t == BAD_TS) return BAD_TS;
      i64 off = (i64)(oh * 60 + om) * 60000;
      return sg == '+' ? t - off : t + off;
    }
  }
  // F3: digits only
  u32 i = 0;
  for (u32 j = 0; j < n; j++) if (!dig(p[j])) return BAD_TS;
  while (i < n && p[i] == '0') i++;
  u32 sig = n - i;
  if (sig > 19) return BAD_TS;  // > int64 max: outside every window
  u64 v = 0;
  for (; i < n; i++) v = v * 10 + (p[i] - '0');
  if (v > (u64)INT64_MAX) return BAD_TS;
  return (i64)v;
}

// ---------------------------------------------------------------- open-addressing string map
struct StrMap {  // key bytes -> u32 value; first 16 key bytes stored inline to avoid a second cache miss
  struct Slot { u64 h; const char* p; u32 n; u32 v; char k[16]; };
  vector<Slot> t; u64 mask = 0; size_t cnt = 0;
  void init(size_t n) { size_t c = 16; while (c < n * 2) c <<= 1; t.assign(c, Slot{0, nullptr, 0, UINT32_MAX, {}}); mask = c - 1; }
  static inline bool eq(const Slot& s, u64 h, const char* p, u32 n) {
    if (s.h != h || s.n != n) return false;
    if (n <= 16) return memcmp(s.k, p, n) == 0;
    return memcmp(s.p, p, n) == 0;
  }
  u32* find_or_insert(u64 h, const char* p, u32 n, bool& inserted) {
    for (u64 i = h & mask;; i = (i + 1) & mask) {
      Slot& s = t[i];
      if (s.v == UINT32_MAX) {
        s.h = h; s.p = p; s.n = n; s.v = 0; memcpy(s.k, p, std::min<u32>(n, 16));
        cnt++; inserted = true; return &s.v;
      }
      if (eq(s, h, p, n)) { inserted = false; return &s.v; }
    }
  }
  const Slot* find(u64 h, const char* p, u32 n) const {
    if (t.empty()) return nullptr;
    for (u64 i = h & mask;; i = (i + 1) & mask) {
      const Slot& s = t[i];
      if (s.v == UINT32_MAX) return nullptr;
      if (eq(s, h, p, n)) return &s;
    }
  }
};

// Exact key -> value index. Keys of the form <prefix><decimal without leading zero> (like p123, c_45)
// go to a flat array; every other key goes to a hash map. A canonical key missing from the array is absent.
struct KeyIndex {
  string prefix; vector<u32> arr; StrMap map;
  static const u32 NONE = UINT32_MAX;
  // returns number, or -1 if not canonical
  inline i64 canon(const char* p, u32 n) const {
    u32 pl = (u32)prefix.size();
    if (n <= pl || n > pl + 9 || memcmp(p, prefix.data(), pl) != 0) return -1;
    if (p[pl] == '0') return -1;
    i64 v = 0;
    for (u32 i = pl; i < n; i++) { if (!dig(p[i])) return -1; v = v * 10 + (p[i] - '0'); }
    return v;
  }
  void build(const vector<std::pair<SV, u32>>& kv) {
    if (!kv.empty()) {
      const SV& k = kv[0].first; u32 j = 0;
      while (j < k.n && !dig(k.p[j])) j++;
      prefix.assign(k.p, j);
    }
    map.init(kv.size() + 1);
    for (auto& [k, v] : kv) {
      i64 c = canon(k.p, k.n);
      if (c >= 0 && c < (1 << 24)) {
        if ((size_t)c >= arr.size()) arr.resize(c + 1, NONE);
        if (arr[c] == NONE) arr[c] = v;
      } else {
        bool ins; u32* slot = map.find_or_insert(hash_bytes(k.p, k.n), k.p, k.n, ins);
        if (ins) *slot = v;
      }
    }
  }
  inline u32 find(const char* p, u32 n) const {
    i64 c = canon(p, n);
    if (c >= 0) return (c < (i64)arr.size() && c < (1 << 24)) ? arr[c] : (c < (1 << 24) ? NONE : find_map(p, n));
    return find_map(p, n);
  }
  inline u32 find_map(const char* p, u32 n) const {
    const StrMap::Slot* s = map.find(hash_bytes(p, n), p, n);
    return s ? s->v : NONE;
  }
};

static inline int bytecmp(const char* a, u32 an, const char* b, u32 bn) {
  int c = memcmp(a, b, std::min(an, bn));
  if (c) return c;
  return an < bn ? -1 : an > bn ? 1 : 0;
}

// ---------------------------------------------------------------- records
struct Person { const char* p; u32 n; u64 h; };
struct ERaw {  // an event copy after parse, keyed for dedup
  u64 h_eid; const char* eid; i64 ingest; i64 ts;
  const char* name; u64 ph; u32 eid_n; u32 name_n; u32 price; uint16_t cat; uint8_t type; uint8_t valid;
};
struct Ev { u64 ph; const char* name; i64 ts; u32 name_n; u32 price; uint16_t cat; uint8_t type; };
struct Ck { u64 ph; const char* name; i64 ts; const char* cid; u32 name_n; u32 cid_n; u32 camp; };

// ---------------------------------------------------------------- Parquet writer
struct Buf {
  vector<uint8_t> b;
  void put(uint8_t c) { b.push_back(c); }
  void put(const void* p, size_t n) { b.insert(b.end(), (const uint8_t*)p, (const uint8_t*)p + n); }
  void varint(u64 v) { while (v >= 0x80) { put(uint8_t(v | 0x80)); v >>= 7; } put(uint8_t(v)); }
  void le32(u32 v) { put(&v, 4); }
  void le64(u64 v) { put(&v, 8); }
};
struct Thrift {
  Buf& o; vector<int> stk; int last = 0;
  explicit Thrift(Buf& b) : o(b) {}
  void field(int id, int type) {
    int d = id - last;
    if (d > 0 && d <= 15) o.put(uint8_t((d << 4) | type));
    else { o.put(uint8_t(type)); o.varint((u64)((id << 1) ^ (id >> 31))); }
    last = id;
  }
  void i32(int id, int32_t v) { field(id, 5); o.varint((u64)(u32)((v << 1) ^ (v >> 31))); }
  void i64f(int id, i64 v) { field(id, 6); o.varint((u64)((v << 1) ^ (v >> 63))); }
  void str(int id, const string& s) { field(id, 8); o.varint(s.size()); o.put(s.data(), s.size()); }
  void begin(int id) { field(id, 12); push(); }
  void push() { stk.push_back(last); last = 0; }
  void end() { o.put(0); last = stk.back(); stk.pop_back(); }
  void list(int id, int etype, size_t n) {
    field(id, 9);
    if (n < 15) o.put(uint8_t((n << 4) | etype)); else { o.put(uint8_t(0xF0 | etype)); o.varint(n); }
  }
  void elem_i32(int32_t v) { o.varint((u64)(u32)((v << 1) ^ (v >> 31))); }
  void elem_str(const string& s) { o.varint(s.size()); o.put(s.data(), s.size()); }
};

enum ColKind { C_STR, C_OPTSTR, C_I64, C_DATE };
struct Col {
  string name; ColKind kind;
  Buf data;            // PLAIN values (non-null)
  vector<uint8_t> def; // for optional: 1 = present
  void add_str(const char* p, u32 n) { data.le32(n); data.put(p, n); if (kind == C_OPTSTR) def.push_back(1); }
  void add_null() { def.push_back(0); }
  void add_i64(i64 v) { data.le64((u64)v); }
  void add_date(int32_t d) { data.le32((u32)d); }
};

static void write_parquet(const string& path, vector<Col>& cols, i64 rows) {
  Buf f;
  f.put("PAR1", 4);
  struct Meta { i64 off, size; };
  vector<Meta> metas;
  if (rows > 0) {
    for (auto& c : cols) {
      Buf page;
      if (c.kind == C_OPTSTR) {
        Buf rle;
        size_t i = 0;
        while (i < c.def.size()) {
          size_t j = i;
          while (j < c.def.size() && c.def[j] == c.def[i]) j++;
          rle.varint((u64)(j - i) << 1);
          rle.put(c.def[i]);
          i = j;
        }
        page.le32((u32)rle.b.size());
        page.put(rle.b.data(), rle.b.size());
      }
      page.put(c.data.b.data(), c.data.b.size());
      Buf hdr; Thrift t(hdr);
      t.i32(1, 0);
      t.i32(2, (int32_t)page.b.size());
      t.i32(3, (int32_t)page.b.size());
      t.begin(5);
      t.i32(1, (int32_t)rows); t.i32(2, 0); t.i32(3, 3); t.i32(4, 3);
      t.end();
      hdr.put(0);
      i64 off = f.b.size();
      f.put(hdr.b.data(), hdr.b.size());
      f.put(page.b.data(), page.b.size());
      metas.push_back({off, (i64)(hdr.b.size() + page.b.size())});
    }
  }
  Buf meta; Thrift t(meta);
  t.i32(1, 1);
  t.list(2, 12, cols.size() + 1);
  t.push(); t.str(4, "schema"); t.i32(5, (int32_t)cols.size()); t.end();
  for (auto& c : cols) {
    t.push();
    int ptype = (c.kind == C_STR || c.kind == C_OPTSTR) ? 6 : c.kind == C_I64 ? 2 : 1;
    t.i32(1, ptype);
    t.i32(3, c.kind == C_OPTSTR ? 1 : 0);
    t.str(4, c.name);
    if (c.kind == C_STR || c.kind == C_OPTSTR) t.i32(6, 0);
    if (c.kind == C_DATE) t.i32(6, 6);
    if (c.kind != C_I64) { t.begin(10); t.begin(c.kind == C_DATE ? 6 : 1); t.end(); t.end(); }
    t.end();
  }
  t.i64f(3, rows);
  if (rows > 0) {
    t.list(4, 12, 1);
    t.push();
    t.list(1, 12, cols.size());
    i64 total = 0;
    for (size_t i = 0; i < cols.size(); i++) {
      auto& c = cols[i];
      int ptype = (c.kind == C_STR || c.kind == C_OPTSTR) ? 6 : c.kind == C_I64 ? 2 : 1;
      t.push();
      t.i64f(2, metas[i].off);
      t.begin(3);
      t.i32(1, ptype);
      t.list(2, 5, 2); t.elem_i32(0); t.elem_i32(3);
      t.list(3, 8, 1); t.elem_str(c.name);
      t.i32(4, 0);
      t.i64f(5, rows);
      t.i64f(6, metas[i].size);
      t.i64f(7, metas[i].size);
      t.i64f(9, metas[i].off);
      t.end();
      t.end();
      total += metas[i].size;
    }
    t.i64f(2, total);
    t.i64f(3, rows);
    t.end();
  } else {
    t.list(4, 12, 0);
  }
  t.str(6, "shopping-sessions-cpp");
  meta.put(0);
  f.put(meta.b.data(), meta.b.size());
  f.le32((u32)meta.b.size());
  f.put("PAR1", 4);
  FILE* fp = fopen(path.c_str(), "wb");
  if (!fp) die(path.c_str());
  if (fwrite(f.b.data(), 1, f.b.size(), fp) != f.b.size()) die("write");
  fclose(fp);
}

// ---------------------------------------------------------------- main
static string env(const char* k, const char* d) { const char* v = getenv(k); return v && *v ? v : d; }

int main(int argc, char** argv) {
  string data = env("DATA_DIR", "/data"), outd = env("OUTPUT_DIR", "/output");
  if (argc >= 3) { data = argv[argc - 2]; outd = argv[argc - 1]; }
  NT = atoi(env("NCPU", "0").c_str());
  if (NT <= 0) NT = (int)std::thread::hardware_concurrency();
  if (NT <= 0) NT = 8;

  // manifest
  i64 ws, we;
  {
    MFile m = map_file(data + "/manifest.json");
    string s(m.p, m.n);
    auto get = [&](const char* key) -> i64 {
      size_t k = s.find(key); if (k == string::npos) die("manifest");
      size_t c = s.find(':', k); size_t q1 = s.find('"', c + 1); size_t q2 = s.find('"', q1 + 1);
      i64 t = parse_ts(s.data() + q1 + 1, u32(q2 - q1 - 1));
      if (t == BAD_TS) die("manifest ts");
      return t;
    };
    ws = get("\"window_start\""); we = get("\"window_end\"");
  }
  const i64 cws = ws - WEEK_MS;
  auto floordiv = [](i64 a, i64 b) { return a >= 0 ? a / b : -((-a + b - 1) / b); };
  const i64 day0 = floordiv(ws, DAY_MS);
  const int ndays = (int)(floordiv(we - 1, DAY_MS) - day0 + 1);

  // products -> category index (indices in byte order of category names; "unknown" included)
  vector<string> cat_names;
  KeyIndex prod_idx;
  uint16_t unknown_cat;
  MFile prod_file = map_file(data + "/products.csv");
  {
    vector<std::pair<SV, SV>> rows;
    Chunk c{prod_file.p, prod_file.n, 0, prod_file.n};
    for_lines(c, [&](const char* s, const char* e) {
      SV f[2];
      if (split_fields(s, e, f, 2) && f[0].n) rows.push_back({f[0], f[1]});
    });
    vector<string_view> cats;
    for (auto& r : rows) cats.emplace_back(r.second.p, r.second.n);
    cats.push_back("unknown");
    std::sort(cats.begin(), cats.end());
    cats.erase(std::unique(cats.begin(), cats.end()), cats.end());
    for (auto& x : cats) cat_names.emplace_back(x);
    auto cat_idx = [&](string_view n) { return (u32)(std::lower_bound(cats.begin(), cats.end(), n) - cats.begin()); };
    unknown_cat = (uint16_t)cat_idx("unknown");
    vector<std::pair<SV, u32>> kv;
    kv.reserve(rows.size());
    for (auto& r : rows) kv.push_back({r.first, cat_idx(string_view(r.second.p, r.second.n))});
    prod_idx.build(kv);
  }
  // campaigns
  vector<string> camp_id, camp_ch;
  KeyIndex camp_idx;
  MFile camp_file = map_file(data + "/campaigns.csv");
  {
    Chunk c{camp_file.p, camp_file.n, 0, camp_file.n};
    vector<std::pair<SV, SV>> rows;
    for_lines(c, [&](const char* s, const char* e) {
      SV f[2];
      if (split_fields(s, e, f, 2) && f[0].n) rows.push_back({f[0], f[1]});
    });
    vector<std::pair<SV, u32>> kv;
    for (auto& r : rows) {
      kv.push_back({r.first, (u32)camp_id.size()});
      camp_id.emplace_back(r.first.p, r.first.n); camp_ch.emplace_back(r.second.p, r.second.n);
    }
    camp_idx.build(kv);
  }
  lap("small tables");
  const u32 NC = (u32)camp_id.size();  // index NC = organic

  // map all inputs
  auto map_all = [&](const string& dir) {
    vector<MFile> v;
    for (auto& p : list_csv(dir)) v.push_back(map_file(p));
    return v;
  };
  vector<MFile> link_files = map_all(data + "/links");
  vector<MFile> click_files = map_all(data + "/clicks");
  vector<MFile> event_files = map_all(data + "/events");
  lap("mapped");

  // ---- links: union-find (runs as one task while the other threads parse events and clicks)
  StrMap id_map;   // linked id -> index
  vector<Person> link_person;  // per linked id index: its person's name
  auto do_links = [&]() {
    vector<std::pair<SV, SV>> pairs;
    for (auto& f : link_files)
      scan_lines<2>(Chunk{f.p, f.n, 0, f.n}, [&](const SV* x) { if (x[0].n && x[1].n) pairs.push_back({x[0], x[1]}); });
    id_map.init(2 * pairs.size() + 16);  // up to 2 distinct ids per link
    vector<SV> ids;
    vector<u64> hs;
    vector<u32> parent;
    ids.reserve(pairs.size() + 16); hs.reserve(pairs.size() + 16); parent.reserve(pairs.size() + 16);
    auto intern = [&](SV s) {
      u64 h = hash_bytes(s.p, s.n);
      bool ins;
      u32* v = id_map.find_or_insert(h, s.p, s.n, ins);
      if (ins) { *v = (u32)ids.size(); ids.push_back(s); hs.push_back(h); parent.push_back(*v); }
      return *v;
    };
    auto find = [&](u32 x) {
      u32 r = x;
      while (parent[r] != r) r = parent[r];
      while (parent[x] != r) { u32 n = parent[x]; parent[x] = r; x = n; }
      return r;
    };
    for (auto& pr : pairs) {
      u32 a = find(intern(pr.first)), b = find(intern(pr.second));
      if (a == b) continue;
      // root = the one with the smaller id string, so roots name their person
      if (bytecmp(ids[a].p, ids[a].n, ids[b].p, ids[b].n) < 0) parent[b] = a; else parent[a] = b;
    }
    link_person.resize(ids.size());
    for (u32 i = 0; i < ids.size(); i++) { u32 r = find(i); link_person[i] = {ids[r].p, ids[r].n, hs[r]}; }
    if (PROF) fprintf(stderr, "links %zu ids %zu\n", pairs.size(), ids.size());
    lap("links done");
  };
  auto resolve = [&](u64 h, const char* p, u32 n) -> Person {
    if (const StrMap::Slot* s = id_map.find(h, p, n)) return link_person[s->v];
    return {p, n, h};
  };

  const int S = 512;  // shards
  struct CRaw { u64 uh; const char* uid; i64 ts; const char* cid; u32 uid_n, cid_n, camp; };
  vector<vector<CRaw>> cr_buf(NT);
  vector<vector<vector<ERaw>>> er_buf(NT, vector<vector<ERaw>>(S));
  auto ck_chunks = make_chunks(click_files, 4 << 20);
  auto ev_chunks = make_chunks(event_files, 4 << 20);
  {
    size_t nck = ck_chunks.size();
    parallel_for(1 + nck + ev_chunks.size(), [&](size_t i, int tid) {
      if (i == 0) { do_links(); return; }
      i--;
      if (i < nck) {
        auto& out = cr_buf[tid];
        scan_lines<4>(ck_chunks[i], [&](const SV* f) {
          if (f[0].n == 0 || f[1].n == 0) return;
          i64 ts = parse_ts(f[3].p, f[3].n);
          if (ts == BAD_TS || ts < cws || ts >= we) return;
          u32 camp = camp_idx.find(f[2].p, f[2].n);
          if (camp == KeyIndex::NONE) return;
          out.push_back(CRaw{hash_bytes(f[1].p, f[1].n), f[1].p, ts, f[0].p, f[1].n, f[0].n, camp});
        });
      } else {
        auto& out = er_buf[tid];
        scan_lines<7>(ev_chunks[i - nck], [&](const SV* f) {
          if (f[0].n == 0) return;
          ERaw r;
          r.h_eid = hash_bytes(f[0].p, f[0].n);
          r.eid = f[0].p; r.eid_n = f[0].n;
          r.ingest = parse_ts(f[3].p, f[3].n);
          if (r.ingest == BAD_TS) r.ingest = INT64_MAX;
          r.valid = 0; r.name = nullptr; r.name_n = 0; r.ph = 0; r.ts = 0; r.price = 0; r.cat = 0; r.type = 0;
          do {
            if (f[1].n == 0 || f[5].n == 0 || r.ingest == INT64_MAX) break;
            i64 ts = parse_ts(f[2].p, f[2].n);
            if (ts == BAD_TS || ts < ws || ts >= we) break;
            const SV& t = f[4];
            uint8_t type;
            if (t.n == 4 && memcmp(t.p, "view", 4) == 0) type = 0;
            else if (t.n == 4 && memcmp(t.p, "cart", 4) == 0) type = 1;
            else if (t.n == 8 && memcmp(t.p, "purchase", 8) == 0) type = 2;
            else break;
            u32 price = 0;
            if (type == 2) {
              const SV& pr = f[6];
              if (pr.n == 0) break;
              u64 v = 0; bool ok = true;
              for (u32 j = 0; j < pr.n; j++) {
                char c = pr.p[j];
                if (!dig(c)) { ok = false; break; }
                v = v * 10 + (c - '0');
                if (v > 10000000) { ok = false; break; }
              }
              if (!ok || v < 1) break;
              price = (u32)v;
            }
            if (r.ingest - ts > DAY_MS) break;
            u32 pc = prod_idx.find(f[5].p, f[5].n);
            r.cat = pc == KeyIndex::NONE ? unknown_cat : (uint16_t)pc;
            // person is resolved after dedup; keep the raw user id here
            r.name = f[1].p; r.name_n = f[1].n; r.ph = hash_bytes(f[1].p, f[1].n);
            r.ts = ts; r.price = price; r.type = type; r.valid = 1;
          } while (0);
          out[(r.h_eid >> 7) % S].push_back(r);
        });
      }
    });
  }

  lap("parse events+clicks");
  // ---- dedup events per eid shard, resolve persons (events and clicks) -> person shards
  vector<vector<vector<Ev>>> ev_buf(NT, vector<vector<Ev>>(S));
  vector<vector<vector<Ck>>> ck_buf(NT, vector<vector<Ck>>(S));
  const int PF = 12;  // prefetch distance for person lookups
  parallel_for(S + NT, [&](size_t s, int tid) {
    if (s >= (size_t)S) {  // clicks of one parse thread
      auto& in = cr_buf[s - S];
      auto& out = ck_buf[tid];
      for (size_t k = 0; k < in.size(); k++) {
        if (k + PF < in.size()) __builtin_prefetch(&id_map.t[in[k + PF].uh & id_map.mask]);
        const CRaw& c = in[k];
        Person pp = resolve(c.uh, c.uid, c.uid_n);
        out[pp.h % S].push_back(Ck{pp.h, pp.p, c.ts, c.cid, pp.n, c.cid_n, c.camp});
      }
      vector<CRaw>().swap(in);
      return;
    }
    size_t n = 0;
    for (int t = 0; t < NT; t++) n += er_buf[t][s].size();
    if (!n) return;
    size_t cap = 16; while (cap < n * 2) cap <<= 1;
    vector<const ERaw*> tab(cap, nullptr);
    u64 mask = cap - 1;
    for (int t = 0; t < NT; t++)
      for (const ERaw& r : er_buf[t][s]) {
        for (u64 i = (r.h_eid >> 20) & mask;; i = (i + 1) & mask) {
          const ERaw*& slot = tab[i];
          if (!slot) { slot = &r; break; }
          if (slot->h_eid == r.h_eid && slot->eid_n == r.eid_n && memcmp(slot->eid, r.eid, r.eid_n) == 0) {
            if (r.ingest < slot->ingest) slot = &r;
            break;
          }
        }
      }
    size_t w = 0;
    for (const ERaw* r : tab) if (r && r->valid) tab[w++] = r;
    auto& out = ev_buf[tid];
    for (size_t k = 0; k < w; k++) {
      if (k + PF < w) __builtin_prefetch(&id_map.t[tab[k + PF]->ph & id_map.mask]);
      const ERaw* r = tab[k];
      Person pp = resolve(r->ph, r->name, r->name_n);
      out[pp.h % S].push_back(Ev{pp.h, pp.p, r->ts, pp.n, r->price, r->cat, r->type});
    }
    for (int t = 0; t < NT; t++) { vector<ERaw>().swap(er_buf[t][s]); }
  });
  er_buf.clear();

  lap("dedup");
  // ---- per person shard: sort, sessionize, attribute, aggregate
  const size_t NCAT = cat_names.size();
  struct CampAgg { vector<i64> sess, purch, rev, assist; };
  vector<CampAgg> cagg(NT);
  for (auto& a : cagg) { size_t z = (size_t)(NC + 1) * ndays; a.sess.assign(z, 0); a.purch.assign(z, 0); a.rev.assign(z, 0); a.assist.assign(z, 0); }
  struct URow { const char* name; u32 n; int32_t day; i64 sessions, events, rev; int32_t top; };
  vector<vector<URow>> urows(NT);

  parallel_for(S, [&](size_t s, int tid) {
    vector<Ev> ev; vector<Ck> ck;
    { size_t n = 0; for (int t = 0; t < NT; t++) n += ev_buf[t][s].size(); ev.reserve(n); }
    for (int t = 0; t < NT; t++) { ev.insert(ev.end(), ev_buf[t][s].begin(), ev_buf[t][s].end()); vector<Ev>().swap(ev_buf[t][s]); }
    { size_t n = 0; for (int t = 0; t < NT; t++) n += ck_buf[t][s].size(); ck.reserve(n); }
    for (int t = 0; t < NT; t++) { ck.insert(ck.end(), ck_buf[t][s].begin(), ck_buf[t][s].end()); vector<Ck>().swap(ck_buf[t][s]); }
    if (ev.empty()) return;
    // Person identity inside a shard = (64-bit hash of the name, name length). Comparing names byte by
    // byte here would touch a random input line per event; a false match needs a 64-bit hash collision
    // between two equal-length names in the same shard (probability ~1e-9 at 60M events).
    auto pkey_cmp = [](u64 ah, const char*, u32 an, u64 bh, const char*, u32 bn) -> int {
      if (ah != bh) return ah < bh ? -1 : 1;
      if (an != bn) return an < bn ? -1 : 1;
      return 0;
    };
    std::sort(ev.begin(), ev.end(), [&](const Ev& a, const Ev& b) {
      if (a.ph != b.ph) return a.ph < b.ph;
      if (a.name_n != b.name_n) return a.name_n < b.name_n;
      return a.ts < b.ts;
    });
    std::sort(ck.begin(), ck.end(), [&](const Ck& a, const Ck& b) {
      if (a.ph != b.ph) return a.ph < b.ph;
      if (a.name_n != b.name_n) return a.name_n < b.name_n;
      if (a.ts != b.ts) return a.ts < b.ts;
      return bytecmp(a.cid, a.cid_n, b.cid, b.cid_n) < 0;
    });
    CampAgg& ca = cagg[tid];
    auto& ur = urows[tid];
    vector<i64> catsum(NCAT, 0);
    vector<int> camp_cnt(NC, 0);
    vector<u32> active; vector<int> active_pos(NC, -1);
    size_t ci = 0, i = 0;
    while (i < ev.size()) {
      size_t j = i + 1;
      while (j < ev.size() && pkey_cmp(ev[i].ph, ev[i].name, ev[i].name_n, ev[j].ph, ev[j].name, ev[j].name_n) == 0) j++;
      // this person's clicks
      while (ci < ck.size() && pkey_cmp(ck[ci].ph, ck[ci].name, ck[ci].name_n, ev[i].ph, ev[i].name, ev[i].name_n) < 0) ci++;
      size_t c0 = ci, c1 = ci;
      while (c1 < ck.size() && pkey_cmp(ck[c1].ph, ck[c1].name, ck[c1].name_n, ev[i].ph, ev[i].name, ev[i].name_n) == 0) c1++;
      ci = c1;
      size_t lo = c0, hi = c0;  // window [lo,hi) of clicks with s-7d <= ts <= s
      // user-day accumulation
      int32_t cur_day = INT32_MIN; i64 u_sess = 0, u_ev = 0, u_rev = 0; bool u_purch = false;
      auto flush_user = [&]() {
        if (cur_day == INT32_MIN) return;
        int32_t top = -1;
        if (u_purch) {
          i64 best = -1;
          for (size_t k = 0; k < NCAT; k++) if (catsum[k] > best) { best = catsum[k]; top = (int32_t)k; }
          std::fill(catsum.begin(), catsum.end(), 0);
        }
        ur.push_back(URow{ev[i].name, ev[i].name_n, cur_day, u_sess, u_ev, u_rev, top});
      };
      size_t k = i;
      while (k < j) {
        i64 start = ev[k].ts;
        size_t m = k + 1;
        while (m < j && ev[m].ts - ev[m - 1].ts <= GAP_MS) m++;
        // session [k,m)
        i64 srev = 0; bool spurch = false;
        for (size_t q = k; q < m; q++)
          if (ev[q].type == 2) { srev += ev[q].price; spurch = true; catsum[ev[q].cat] += ev[q].price; }
        int32_t day = (int32_t)floordiv(start, DAY_MS);
        if (day != cur_day) {
          // flush previous day (catsum belongs to it, so flush before adding this session's sums)
          if (cur_day != INT32_MIN) {
            // remove this session's contribution, flush, re-add
            for (size_t q = k; q < m; q++) if (ev[q].type == 2) catsum[ev[q].cat] -= ev[q].price;
            flush_user();
            for (size_t q = k; q < m; q++) if (ev[q].type == 2) catsum[ev[q].cat] += ev[q].price;
          }
          cur_day = day; u_sess = 0; u_ev = 0; u_rev = 0; u_purch = false;
        }
        u_sess++; u_ev += (i64)(m - k); u_rev += srev; u_purch |= spurch;
        // attribution window
        while (hi < c1 && ck[hi].ts <= start) {
          u32 cp = ck[hi].camp;
          if (camp_cnt[cp]++ == 0) { active_pos[cp] = (int)active.size(); active.push_back(cp); }
          hi++;
        }
        while (lo < hi && ck[lo].ts < start - WEEK_MS) {
          u32 cp = ck[lo].camp;
          if (--camp_cnt[cp] == 0) {
            int pos = active_pos[cp]; u32 last = active.back();
            active[pos] = last; active_pos[last] = pos; active.pop_back(); active_pos[cp] = -1;
          }
          lo++;
        }
        u32 credit = NC;
        if (hi > lo) {
          size_t a = hi - 1;
          i64 t = ck[a].ts;
          while (a > lo && ck[a - 1].ts == t) a--;
          credit = ck[a].camp;
        }
        size_t di = (size_t)(day - day0);
        size_t idx = (size_t)credit * ndays + di;
        ca.sess[idx]++; ca.rev[idx] += srev; if (spurch) ca.purch[idx]++;
        for (u32 cp : active) ca.assist[(size_t)cp * ndays + di]++;
        k = m;
      }
      flush_user();
      // reset window state
      for (u32 cp : active) { camp_cnt[cp] = 0; active_pos[cp] = -1; }
      active.clear();
      i = j;
    }
  });

  lap("sessions");
  // ---- outputs
  string ud = outd + "/user_daily", cd = outd + "/campaign_daily";
  mkdir(outd.c_str(), 0777); mkdir(ud.c_str(), 0777); mkdir(cd.c_str(), 0777);
  std::thread cw([&] {
    for (int t = 1; t < NT; t++)
      for (size_t z = 0; z < cagg[0].sess.size(); z++) {
        cagg[0].sess[z] += cagg[t].sess[z]; cagg[0].purch[z] += cagg[t].purch[z];
        cagg[0].rev[z] += cagg[t].rev[z]; cagg[0].assist[z] += cagg[t].assist[z];
      }
    vector<Col> cols = {{"campaign_id", C_STR, {}, {}}, {"channel", C_STR, {}, {}}, {"day", C_DATE, {}, {}},
                        {"sessions", C_I64, {}, {}}, {"purchasing_sessions", C_I64, {}, {}},
                        {"revenue_cents", C_I64, {}, {}}, {"assisted_sessions", C_I64, {}, {}}};
    i64 rows = 0;
    const string organic = "organic";
    for (u32 c = 0; c <= NC; c++)
      for (int d = 0; d < ndays; d++) {
        size_t z = (size_t)c * ndays + d;
        auto& a = cagg[0];
        if (!a.sess[z] && !a.assist[z]) continue;
        const string& id = c == NC ? organic : camp_id[c];
        const string& ch = c == NC ? organic : camp_ch[c];
        cols[0].add_str(id.data(), (u32)id.size()); cols[1].add_str(ch.data(), (u32)ch.size());
        cols[2].add_date((int32_t)(day0 + d));
        cols[3].add_i64(a.sess[z]); cols[4].add_i64(a.purch[z]); cols[5].add_i64(a.rev[z]); cols[6].add_i64(a.assist[z]);
        rows++;
      }
    write_parquet(cd + "/part-0.parquet", cols, rows);
  });
  parallel_for(NT, [&](size_t t, int) {
    auto& rs = urows[t];
    if (rs.empty() && t != 0) return;
    vector<Col> cols = {{"user_id", C_STR, {}, {}}, {"day", C_DATE, {}, {}}, {"sessions", C_I64, {}, {}},
                        {"events", C_I64, {}, {}}, {"revenue_cents", C_I64, {}, {}}, {"top_category", C_OPTSTR, {}, {}}};
    for (auto& r : rs) {
      cols[0].add_str(r.name, r.n); cols[1].add_date(r.day); cols[2].add_i64(r.sessions);
      cols[3].add_i64(r.events); cols[4].add_i64(r.rev);
      if (r.top >= 0) cols[5].add_str(cat_names[r.top].data(), (u32)cat_names[r.top].size()); else cols[5].add_null();
    }
    write_parquet(ud + "/part-" + std::to_string(t) + ".parquet", cols, (i64)rs.size());
  });
  cw.join();
  lap("write");
  fflush(stdout);
  _exit(0);  // skip destructors / munmap
}
