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
#include <cstddef>
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
static int DEXP = getenv("SS_DEXP") ? atoi(getenv("SS_DEXP")) : 0;
static std::atomic<unsigned long long> dsink{0};
#include <sys/resource.h>
static void lap(const char* what) {
  if (!PROF) return;
  static double lu = 0, ls = 0;
  double ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - T0).count();
  rusage ru; getrusage(RUSAGE_SELF, &ru);
  double u = ru.ru_utime.tv_sec * 1e3 + ru.ru_utime.tv_usec / 1e3, sy = ru.ru_stime.tv_sec * 1e3 + ru.ru_stime.tv_usec / 1e3;
  fprintf(stderr, "%8.1f ms  %s  cpu+%.0f user +%.0f sys\n", ms, what, u - lu, sy - ls);
  lu = u; ls = sy;
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
static inline u64 ld8u(const char* p) { u64 v; __builtin_memcpy(&v, p, 8); return v; }
static inline u32 ld4u(const char* p) { u32 v; __builtin_memcpy(&v, p, 4); return v; }
// Pure function of the bytes; never reads outside [p, p+n).
template <u64 SEED> static inline u64 hash_seeded(const char* p, u32 n) {
  u64 h = SEED ^ (u64(n) * 0xff51afd7ed558ccdULL);
  if (n >= 8) {
    const char* e = p + n;
    while (e - p > 8) { h = (h ^ mix64(ld8u(p))) * 0x9E3779B97F4A7C15ULL; p += 8; }
    h = (h ^ mix64(ld8u(e - 8) ^ 0x5bd1e995ULL)) * 0x9E3779B97F4A7C15ULL;
  } else if (n >= 4) {
    u64 v = (u64)ld4u(p) | ((u64)ld4u(p + n - 4) << 32);
    h = (h ^ mix64(v)) * 0x9E3779B97F4A7C15ULL;
  } else if (n > 0) {
    u64 v = (u64)(uint8_t)p[0] | ((u64)(uint8_t)p[n / 2] << 8) | ((u64)(uint8_t)p[n - 1] << 16);
    h = (h ^ mix64(v)) * 0x9E3779B97F4A7C15ULL;
  }
  return mix64(h);
}
static inline u64 hash_bytes(const char* p, u32 n) { return hash_seeded<0x9E3779B97F4A7C15ULL>(p, n); }
static inline u64 hash_bytes2(const char* p, u32 n) { return hash_seeded<0xC2B2AE3D27D4EB4FULL>(p, n); }

// ---------------------------------------------------------------- arena of 16 KB blocks, recycled per thread
static const size_t BLK_BYTES = 16384;
static char* g_arena = nullptr;
static size_t g_arena_cap = 0;
static std::atomic<size_t> g_arena_off{0};
static thread_local char* tl_cur = nullptr;
static thread_local char* tl_end = nullptr;
static thread_local vector<void*>* tl_free = nullptr;  // blocks this thread may reuse (already faulted in)
static void arena_init(size_t c) {
  g_arena_cap = c;
  void* m = mmap(nullptr, c, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
  if (m == MAP_FAILED) { fprintf(stderr, "arena mmap failed\n"); exit(1); }
  madvise(m, c, MADV_HUGEPAGE);  // 2 MB pages: far fewer faults and a faster teardown where THP is available
  g_arena = (char*)m;
}
static void* block_alloc() {
  if (tl_free && !tl_free->empty()) { void* b = tl_free->back(); tl_free->pop_back(); return b; }
  if (tl_cur == tl_end) {
    const size_t slab = 4 << 20;
    size_t o = g_arena_off.fetch_add(slab);
    if (o + slab > g_arena_cap) { fprintf(stderr, "arena exhausted\n"); exit(1); }
    tl_cur = g_arena + o; tl_end = tl_cur + slab;
  }
  void* r = tl_cur; tl_cur += BLK_BYTES; return r;
}
// Append-only list of 16 KB blocks; items never move. Pushes are staged in a small cached buffer and
// written to the block in whole 256-byte chunks with non-temporal stores (no read-for-ownership miss).
// After the writing phase: the writer calls _mm_sfence(), then finish() publishes the staged tail.
template <class T> struct BList {
  static const u32 HDR = 64;
  static const u32 AREA = (u32)(((BLK_BYTES - HDR) / 256) * 256);
  static const u32 CAP = (u32)(AREA / sizeof(T));
  struct Blk {
    Blk* next; u32 n; u32 pad[13];
    alignas(64) char data[AREA];
    inline T* items() { return (T*)data; }
    inline const T* items() const { return (const T*)data; }
  };
  static_assert(sizeof(Blk) <= BLK_BYTES && offsetof(Blk, data) == HDR);
  Blk* head = nullptr; Blk* tail = nullptr; size_t total = 0;
  char* dst = nullptr; u32 soff = 0; u32 tn = CAP;  // tn = items in tail (kept here, not in the cold block header)
  alignas(64) char stage[256 + sizeof(T)];
  inline void flush_stage() { if (soff) { memcpy(dst, stage, soff); soff = 0; } }
  inline void push(const T& x) {
    if (tn == CAP) {
      flush_stage();
      if (tail) tail->n = CAP;
      Blk* b = (Blk*)block_alloc(); b->next = nullptr; b->n = 0;
      if (tail) tail->next = b; else head = b;
      tail = b; dst = b->data; tn = 0;
    }
    memcpy(stage + soff, &x, sizeof(T)); soff += sizeof(T);
    tn++; total++;
    if (soff >= 256) {
      for (int i = 0; i < 8; i++)
        _mm256_stream_si256((__m256i*)(dst + 32 * i), _mm256_load_si256((const __m256i*)(stage + 32 * i)));
      dst += 256; soff -= 256;
      if (soff) memcpy(stage, stage + 256, soff);
    }
  }
  inline void finish() { flush_stage(); if (tail) tail->n = tn; }
  template <class F> inline void each(F f) const {
    for (Blk* b = head; b; b = b->next) {
      if (b->next) _mm_prefetch((const char*)b->next, _MM_HINT_T0);
      const T* it = b->items();
      const char* end = (const char*)(it + b->n);
      for (u32 i = 0; i < b->n; i++) {
        const char* ahead = (const char*)(it + i) + 1024;  // stream ahead within the block
        if (ahead < end) _mm_prefetch(ahead, _MM_HINT_T0);
        f(it[i]);
      }
    }
  }
  inline void append_to(vector<T>& v) const { for (Blk* b = head; b; b = b->next) v.insert(v.end(), b->items(), b->items() + b->n); }
  // hand every block to the calling thread's free list (the list must not be used afterwards)
  inline void recycle() { for (Blk* b = head; b;) { Blk* n = b->next; if (tl_free) tl_free->push_back(b); b = n; } head = tail = nullptr; total = 0; dst = nullptr; soff = 0; tn = CAP; }
};

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
struct MFile { const char* p = nullptr; size_t n = 0; int fd = -1; };  // p = mapping, or fd for pread
static MFile open_file(const string& path) {  // for pread: no mapping
  MFile f;
  f.fd = open(path.c_str(), O_RDONLY);
  if (f.fd < 0) die(path.c_str());
  struct stat st; fstat(f.fd, &st);
  f.n = st.st_size;
  return f;
}
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
struct Chunk { const char* base; size_t fsize, b, e; int fd = -1; };
static vector<Chunk> make_chunks(const vector<MFile>& files, size_t target) {
  vector<Chunk> cs;
  for (auto& f : files)
    for (size_t b = 0; b < f.n; b += target) cs.push_back({f.p, f.n, b, std::min(f.n, b + target), f.fd});
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
// Per-line SIMD scanner (fast path for lines < 128 bytes not near EOF): one pass of 4 AVX2 loads gives
// newline, comma and quote bitmasks; fields come from comma positions, and a line is well formed iff it
// has K-1 commas and every quote is the first or last byte of a field wrapped in a pair of quotes.
template <int K> static inline bool fields_from_masks(const char* p, u32 len, u64 c0, u64 c1, u64 q0, u64 q1, SV* f) {
  if (__builtin_popcountll(c0) + __builtin_popcountll(c1) != K - 1) return false;
  // field boundaries: exactly K-1 comma positions (fixed trip count, no data-dependent branches), then len
  u32 ends[K];
  for (int k = 0; k < K - 1; k++) {
    const bool lo = c0 != 0;
    const u64 w = lo ? c0 : c1;
    const u32 bit = (u32)__builtin_ctzll(w | (1ULL << 63));  // w != 0 here; the OR keeps ctz defined
    ends[k] = bit + (lo ? 0 : 64);
    const u64 cleared = w & (w - 1);
    c0 = lo ? cleared : c0;
    c1 = lo ? c1 : cleared;
  }
  ends[K - 1] = len;
  const u32 nq = (u32)(__builtin_popcountll(q0) + __builtin_popcountll(q1));
  // branch-free, from the masks: a field is quoted iff its first byte is '"'; then its last byte must
  // be '"' too and n >= 2. Every quote in the line must be one of these.
  auto qbit = [&](u32 x) -> u32 { u64 w = x < 64 ? q0 : q1; return (u32)((w >> (x & 63)) & 1); };
  u32 a = 0, bq = 0, ok = 1;
  for (int i = 0; i < K; i++) {
    const u32 e = ends[i], n = e - a;
    const u32 q = n ? qbit(a) : 0;
    const u32 last = n ? qbit(e - 1) : 0;
    ok &= (q ^ 1) | ((n >= 2) & last);
    f[i] = {p + a + q, n - 2 * q};
    bq += 2 * q;
    a = e + 1;
  }
  return ok && nq == bq;
}
static const bool POPULATE = getenv("SS_NOPOP") == nullptr;
static const int PF_DIST = getenv("SS_PFD") ? atoi(getenv("SS_PFD")) : 1024;  // input prefetch distance (bytes)
template <int K, class F> static inline void scan_lines(const Chunk& c, F fn) {
  const char* base = c.base;
  const size_t n = c.fsize;
  if (POPULATE && c.fd < 0) {  // map this chunk's file pages in one call instead of one fault per few pages
    uintptr_t a0 = (uintptr_t)(base + c.b) & ~(uintptr_t)4095;
    uintptr_t a1 = std::min((uintptr_t)(base + n), (uintptr_t)(base + c.e) + 8192);
    if (a1 > a0) madvise((void*)a0, a1 - a0, 22 /* MADV_POPULATE_READ */);
  }
  size_t pos;
  {
    const char* nl = (const char*)memchr(base + (c.b ? c.b - 1 : 0), '\n', n - (c.b ? c.b - 1 : 0));
    if (!nl) return;
    pos = nl - base + 1;
  }
  const __m256i vc = _mm256_set1_epi8(','), vq = _mm256_set1_epi8('"'), vn = _mm256_set1_epi8('\n');
  SV f[K];
  while (pos < c.e) {
    const char* p = base + pos;
    // The hardware prefetcher stops at 4 KB boundaries; pull the input in ~1 KB ahead ourselves.
    // (Prefetches never fault, so running past the end of the mapping is harmless.)
    _mm_prefetch(p + PF_DIST, _MM_HINT_T0);
    _mm_prefetch(p + PF_DIST + 64, _MM_HINT_T0);
    if (pos + 128 <= n) {
      __m256i x0 = _mm256_loadu_si256((const __m256i*)p), x1 = _mm256_loadu_si256((const __m256i*)(p + 32));
      u64 n0 = (u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(x0, vn)) | ((u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(x1, vn)) << 32);
      u64 c0 = (u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(x0, vc)) | ((u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(x1, vc)) << 32);
      u64 q0 = (u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(x0, vq)) | ((u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(x1, vq)) << 32);
      if (n0) {
        u32 len = (u32)__builtin_ctzll(n0);
        u64 m = len ? (~0ULL >> (64 - len)) : 0;
        if (fields_from_masks<K>(p, len, c0 & m, 0, q0 & m, 0, f)) fn(f);
        pos += len + 1;
        continue;
      }
      __m256i x2 = _mm256_loadu_si256((const __m256i*)(p + 64)), x3 = _mm256_loadu_si256((const __m256i*)(p + 96));
      u64 n1 = (u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(x2, vn)) | ((u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(x3, vn)) << 32);
      if (n1) {
        u64 c1 = (u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(x2, vc)) | ((u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(x3, vc)) << 32);
        u64 q1 = (u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(x2, vq)) | ((u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(x3, vq)) << 32);
        u32 len = 64 + (u32)__builtin_ctzll(n1);
        u64 m = (len - 64) ? (~0ULL >> (128 - len)) : 0;
        if (fields_from_masks<K>(p, len, c0, c1 & m, q0, q1 & m, f)) fn(f);
        pos += len + 1;
        continue;
      }
    }
    // slow path: long line or close to the end of the file
    const char* nl = (const char*)memchr(p, '\n', n - pos);
    const char* e = nl ? nl : base + n;
    if (split_fields(p, e, f, K)) fn(f);
    pos = (e - base) + 1;
  }
}

// Reads chunk c of a pread file into a reused per-thread buffer and returns the equivalent in-buffer chunk
// (same lines, same header rule). The buffer extends past c.e to the end of the last line starting in c.
static thread_local vector<char> tl_rbuf;
static Chunk load_chunk(const Chunk& c) {
  const size_t start = c.b ? c.b - 1 : 0;
  size_t extra = 64 << 10;
  for (;;) {
    const size_t end = std::min(c.fsize, c.e + extra);
    const size_t len = end - start;
    if (tl_rbuf.size() < len) tl_rbuf.resize(len + (64 << 10));
    size_t done = 0;
    while (done < len) {
      ssize_t r = pread(c.fd, tl_rbuf.data() + done, len - done, (off_t)(start + done));
      if (r <= 0) die("pread");
      done += (size_t)r;
    }
    // the last line starting before c.e must end inside the buffer (or the file must end there)
    if (end == c.fsize || c.e == c.b || memchr(tl_rbuf.data() + (c.e - 1 - start), '\n', end - (c.e - 1)) != nullptr)
      return Chunk{tl_rbuf.data(), len, c.b - start, c.e - start, -1};
    extra *= 4;
  }
}
// Bump allocator for the few input bytes that must outlive the read buffer (user ids, click ids).
static thread_local char* tl_scur = nullptr;
static thread_local char* tl_send = nullptr;
static inline const char* keep_str(const char* p, u32 n) {
  if ((size_t)(tl_send - tl_scur) < n) {
    if (n > 8192) { char* big = (char*)malloc(n); memcpy(big, p, n); return big; }
    tl_scur = (char*)block_alloc(); tl_send = tl_scur + BLK_BYTES;
  }
  char* d = tl_scur;
  if (n >= 8 && n <= 16 && (size_t)(tl_send - tl_scur) >= 16) {  // ids are ~9 bytes: two overlapping 8-byte moves
    u64 a, b; __builtin_memcpy(&a, p, 8); __builtin_memcpy(&b, p + n - 8, 8);
    __builtin_memcpy(d, &a, 8); __builtin_memcpy(d + n - 8, &b, 8);
  } else memcpy(d, p, n);
  tl_scur += n; return d;
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
// Calendar tables for years [DT_Y0, DT_Y0 + DT_NY); other years use days_from_civil.
static const int DT_Y0 = 1900, DT_NY = 300;
struct DateTables {
  int32_t jan1[DT_NY]; uint8_t leap[DT_NY]; int cum[2][12]; int dim[2][12];
  DateTables() {
    static const int dm[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    for (int l = 0; l < 2; l++) {
      int c = 0;
      for (int m = 0; m < 12; m++) { dim[l][m] = dm[m] + (l && m == 1); cum[l][m] = c; c += dim[l][m]; }
    }
    for (int i = 0; i < DT_NY; i++) {
      int y = DT_Y0 + i;
      leap[i] = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
      jan1[i] = (int32_t)days_from_civil(y, 1, 1);
    }
  }
};
static const DateTables g_dt;
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
  i64 days;
  if (y >= DT_Y0 && y < DT_Y0 + DT_NY) {  // table path
    const int leap = g_dt.leap[y - DT_Y0];
    if (d > g_dt.dim[leap][mo - 1]) return BAD_TS;
    days = g_dt.jan1[y - DT_Y0] + g_dt.cum[leap][mo - 1] + d - 1;
  } else {
    static const int dim[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    int md = dim[mo - 1];
    if (mo == 2 && ((y % 4 == 0 && y % 100 != 0) || y % 400 == 0)) md = 29;
    if (d > md) return BAD_TS;
    days = days_from_civil(y, mo, d);
  }
  return (days * 86400 + h * 3600 + mi * 60 + sec) * 1000;
}
// 8 ASCII digits (first digit in the lowest byte) -> value
static inline u32 swar8(u64 v) {
  v -= 0x3030303030303030ULL;
  v = (v * 10) + (v >> 8);
  v = (((v & 0x000000FF000000FFULL) * (100 + (1000000ULL << 32))) + (((v >> 16) & 0x000000FF000000FFULL) * (1 + (10000ULL << 32)))) >> 32;
  return (u32)v;
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
  if (n == 13) {  // the usual epoch-ms width: SWAR check + convert
    const u64 a = ld8(p), b = ld8(p + 5);
    if (!digits8(a, ~0ULL) || !digits8(b, ~0ULL)) return BAD_TS;
    return (i64)swar8(a) * 100000 + (i64)(swar8(b) % 100000);
  }
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
  static constexpr u32 NONE = UINT32_MAX;
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
    size_t noncanon = 0;
    for (auto& [k, v] : kv) { i64 c = canon(k.p, k.n); if (!(c >= 0 && c < (1 << 24))) noncanon++; }
    map.init(noncanon + 1);  // only keys that do not fit the flat array
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
  // Same result as find(), for keys inside a read buffer with at least 8 readable bytes after the key's
  // prefix (true for parse buffers, which carry 64 KB of slack). Digits are validated and converted 8 at a
  // time; anything unusual takes the exact path.
  inline u32 find_fast(const char* p, u32 n) const {
    const u32 pl = (u32)prefix.size();
    const u32 m = n - pl;  // digit count
    if (n > pl && m <= 8 && (pl == 0 || p[0] == prefix[0]) && (pl < 2 || p[1] == prefix[1]) && pl <= 2) {
      const u64 mask = m == 8 ? ~0ULL : ((1ULL << (8 * m)) - 1);
      const u64 v = ld8u(p + pl) & mask;
      if (digits8(v, mask) && p[pl] != '0') {
        // left-pad with '0' digits so the value is right-aligned in 8 digits
        const u64 v8 = m == 8 ? v : ((v << (8 * (8 - m))) | (0x3030303030303030ULL >> (8 * m)));
        const u32 c = swar8(v8);
        if (c < (1u << 24)) return c < arr.size() ? arr[c] : NONE;
      }
    }
    return find(p, n);
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
// Pointer (user space, < 2^48) and a 16-bit length packed in one word.
static inline u64 pack_ref(const char* p, u32 n) { return (u64)(uintptr_t)p | ((u64)n << 48); }
static inline const char* ref_p(u64 r) { return (const char*)(uintptr_t)(r & 0xFFFFFFFFFFFFULL); }
static inline u32 ref_n(u64 r) { return (u32)(r >> 48); }
// Timestamps inside records are u32 milliseconds since TBASE (= window_start - 7 days).
struct ERaw {  // an event copy after parse, keyed for dedup (40 bytes)
  u64 h_eid;      // event_id hash; identity = h_eid + chk (see SOLUTION.md)
  u64 uref;       // user_id pointer + length
  u64 uh;         // user_id hash (computed while the bytes are in cache)
  int32_t ingest; // ingest_ts in seconds since window_start (clamped to int32)
  u32 ts;
  u32 pk;         // price (24 bits) | type << 24 | valid << 26
  uint16_t cat;
  uint16_t chk;   // second, independent 16-bit hash of event_id
};
struct Ev { u64 ph; u64 nref; u32 ts; u32 price; uint16_t cat; uint8_t type; uint8_t pad; };  // 32 bytes
struct Ck { u64 ph; const char* cid; u32 ts; u32 camp; uint16_t cid_n; uint16_t name_n; u32 pad; };  // 32 bytes
static_assert(sizeof(ERaw) == 40 && sizeof(Ev) == 32 && sizeof(Ck) == 32);

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
  // Streams the file piece by piece: column buffers are written as they are, never copied again.
  FILE* fp = fopen(path.c_str(), "wb");
  if (!fp) die(path.c_str());
  i64 fpos = 0;
  auto out = [&](const void* p, size_t n) { if (n && fwrite(p, 1, n, fp) != n) die("write"); fpos += (i64)n; };
  out("PAR1", 4);
  struct Meta { i64 off, size; };
  vector<Meta> metas;
  if (rows > 0) {
    for (auto& c : cols) {
      Buf lev;  // definition levels (optional column): 4-byte length + RLE runs
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
        lev.le32((u32)rle.b.size());
        lev.put(rle.b.data(), rle.b.size());
      }
      const size_t psize = lev.b.size() + c.data.b.size();
      Buf hdr; Thrift t(hdr);
      t.i32(1, 0);
      t.i32(2, (int32_t)psize);
      t.i32(3, (int32_t)psize);
      t.begin(5);
      t.i32(1, (int32_t)rows); t.i32(2, 0); t.i32(3, 3); t.i32(4, 3);
      t.end();
      hdr.put(0);
      i64 off = fpos;
      out(hdr.b.data(), hdr.b.size());
      out(lev.b.data(), lev.b.size());
      out(c.data.b.data(), c.data.b.size());
      metas.push_back({off, (i64)(hdr.b.size() + psize)});
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
  out(meta.b.data(), meta.b.size());
  u32 ml = (u32)meta.b.size();
  out(&ml, 4);
  out("PAR1", 4);
  if (fclose(fp) != 0) die("close");
}


// ---------------------------------------------------------------- optional sampling profiler (SS_SAMPLE=path)
#include <csignal>
#include <sys/time.h>
#include <ucontext.h>
static uintptr_t* g_samp = nullptr;
static std::atomic<size_t> g_nsamp{0};
static const size_t MAXSAMP = 1 << 22;
static void on_prof(int, siginfo_t*, void* ctx) {
  size_t i = g_nsamp.fetch_add(1, std::memory_order_relaxed);
  if (i < MAXSAMP) g_samp[i] = (uintptr_t)((ucontext_t*)ctx)->uc_mcontext.gregs[REG_RIP];
}
static void sample_start() {
  if (!getenv("SS_SAMPLE")) return;
  g_samp = (uintptr_t*)calloc(MAXSAMP, sizeof(uintptr_t));
  struct sigaction sa{}; sa.sa_sigaction = on_prof; sa.sa_flags = SA_SIGINFO | SA_RESTART;
  bool user = getenv("SS_SAMPLE_USER") != nullptr;
  sigaction(user ? SIGVTALRM : SIGPROF, &sa, nullptr);
  itimerval tv{{0, 200}, {0, 200}};
  setitimer(user ? ITIMER_VIRTUAL : ITIMER_PROF, &tv, nullptr);
}
static void sample_stop() {
  const char* path = getenv("SS_SAMPLE");
  if (!path) return;
  itimerval tv{}; setitimer(ITIMER_PROF, &tv, nullptr); setitimer(ITIMER_VIRTUAL, &tv, nullptr);
  FILE* f = fopen(path, "w");
  size_t n = std::min(g_nsamp.load(), MAXSAMP);
  extern char __executable_start;
  for (size_t i = 0; i < n; i++) fprintf(f, "%lx\n", (unsigned long)(g_samp[i] - (uintptr_t)&__executable_start));
  fclose(f);
}

// ---------------------------------------------------------------- main
static string env(const char* k, const char* d) { const char* v = getenv(k); return v && *v ? v : d; }

// Profile-guided builds link src/pgo_gen.cpp, which points this at libgcov's dump (_exit skips the normal
// one). Every other build leaves it null, so main() is identical in both, as the profile requires.
void (*g_before_exit)() = nullptr;
int main(int argc, char** argv) {
  sample_start();
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
  const i64 TBASE = cws;  // record timestamps are u32 ms offsets from here
  if (we - TBASE >= ((i64)1 << 32)) die("window longer than 42 days is not supported");
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
    // distinct categories via a small map (few categories, many products), then byte order
    std::unordered_map<string_view, u32> first_id;
    vector<string_view> cats;
    vector<u32> row_cat(rows.size());
    for (size_t i = 0; i < rows.size(); i++) {
      string_view nm(rows[i].second.p, rows[i].second.n);
      auto it = first_id.find(nm);
      if (it == first_id.end()) { it = first_id.emplace(nm, (u32)cats.size()).first; cats.push_back(nm); }
      row_cat[i] = it->second;
    }
    if (!first_id.count("unknown")) { first_id.emplace("unknown", (u32)cats.size()); cats.push_back("unknown"); }
    vector<u32> order(cats.size());
    for (u32 i = 0; i < order.size(); i++) order[i] = i;
    std::sort(order.begin(), order.end(), [&](u32 a, u32 b) { return cats[a] < cats[b]; });
    vector<u32> rank(cats.size());
    for (u32 r = 0; r < order.size(); r++) { rank[order[r]] = r; cat_names.emplace_back(cats[order[r]]); }
    unknown_cat = (uint16_t)rank[first_id["unknown"]];
    vector<std::pair<SV, u32>> kv;
    kv.reserve(rows.size());
    for (size_t i = 0; i < rows.size(); i++) kv.push_back({rows[i].first, rank[row_cat[i]]});
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
  auto open_all = [&](const string& dir) {
    vector<MFile> v;
    for (auto& p : list_csv(dir)) v.push_back(open_file(p));
    return v;
  };
  vector<MFile> click_files = open_all(data + "/clicks");
  vector<MFile> event_files = open_all(data + "/events");
  if (!getenv("SS_NORA")) {
    // If the inputs are not in the page cache, queue reads for all of them now, in processing order, so the
    // disk sees a deep queue of large requests while the workers parse. Cheap when the data is cached.
    std::thread([data] {
      for (const char* sub : {"/links", "/clicks", "/events"})
        for (auto& path : list_csv(data + sub)) {
          int fd = open(path.c_str(), O_RDONLY);
          if (fd < 0) continue;
          posix_fadvise(fd, 0, 0, POSIX_FADV_WILLNEED);
          close(fd);
        }
    }).detach();
  }
  {
    size_t in_bytes = 0;
    for (auto* v : {&link_files, &click_files, &event_files}) for (auto& f : *v) in_bytes += f.n;
    arena_init(in_bytes * 3 + ((size_t)1 << 30));
  }
  lap("mapped");

  // ---- links: union-find (runs as one task while the other threads parse events and clicks)
  struct PSlot { u64 h; u32 n; u32 used; u64 pref; u64 ph; };  // 32 bytes: id hash/len -> person name ref + hash
  vector<PSlot> ptab;  // linked id -> person, one cache line per lookup
  u64 pmask = 0;
  auto do_links = [&]() {
    vector<std::pair<SV, SV>> pairs;
    for (auto& f : link_files)
      scan_lines<2>(Chunk{f.p, f.n, 0, f.n}, [&](const SV* x) { if (x[0].n && x[1].n) pairs.push_back({x[0], x[1]}); });
    const size_t np = pairs.size();
    vector<u64> hsd(2 * np);
    for (size_t k = 0; k < np; k++) {
      hsd[2 * k] = hash_bytes(pairs[k].first.p, pairs[k].first.n);
      hsd[2 * k + 1] = hash_bytes(pairs[k].second.p, pairs[k].second.n);
    }
    // id table: identity = 64-bit hash + length (same rule as resolve)
    struct LS { u64 h; u32 n; u32 v; };
    size_t cap = 16; while (cap < 3 * np + 16) cap <<= 1;
    vector<LS> lt(cap, LS{0, 0, UINT32_MAX});
    const u64 lmask = cap - 1;
    vector<SV> ids; vector<u64> hs, pre; vector<u32> parent;
    ids.reserve(np + 16); hs.reserve(np + 16); pre.reserve(np + 16); parent.reserve(np + 16);
    auto prefix = [](SV s) {  // first 8 bytes, big-endian, zero padded: orders like bytecmp unless equal
      u64 v = 0;
      for (u32 i = 0; i < 8; i++) v = (v << 8) | (i < s.n ? (uint8_t)s.p[i] : 0);
      return v;
    };
    auto intern = [&](u64 h, SV s) {
      for (u64 j = h & lmask;; j = (j + 1) & lmask) {
        LS& e = lt[j];
        if (e.v == UINT32_MAX) {
          e = LS{h, s.n, (u32)ids.size()};
          ids.push_back(s); hs.push_back(h); pre.push_back(prefix(s)); parent.push_back(e.v);
          return e.v;
        }
        if (e.h == h && e.n == s.n) return e.v;
      }
    };
    auto find = [&](u32 x) {
      u32 r = x;
      while (parent[r] != r) r = parent[r];
      while (parent[x] != r) { u32 n = parent[x]; parent[x] = r; x = n; }
      return r;
    };
    auto less_id = [&](u32 a, u32 b) {
      if (pre[a] != pre[b]) return pre[a] < pre[b];
      return bytecmp(ids[a].p, ids[a].n, ids[b].p, ids[b].n) < 0;
    };
    const size_t LPF = 16;
    for (size_t k = 0; k < np; k++) {
      if (k + LPF < np) { __builtin_prefetch(&lt[hsd[2 * (k + LPF)] & lmask]); __builtin_prefetch(&lt[hsd[2 * (k + LPF) + 1] & lmask]); }
      u32 a = find(intern(hsd[2 * k], pairs[k].first)), b = find(intern(hsd[2 * k + 1], pairs[k].second));
      if (a == b) continue;
      // root = the one with the smaller id string, so roots name their person
      if (less_id(a, b)) parent[b] = a; else parent[a] = b;
    }
    vector<LS>().swap(lt);
    size_t pcap = 16; while (pcap < ids.size() * 2) pcap <<= 1;
    ptab.assign(pcap, PSlot{0, 0, 0, 0, 0});
    pmask = pcap - 1;
    for (u32 i = 0; i < ids.size(); i++) {
      u32 r = find(i);
      for (u64 j = hs[i] & pmask;; j = (j + 1) & pmask)
        if (!ptab[j].used) { ptab[j] = PSlot{hs[i], ids[i].n, 1, pack_ref(ids[r].p, ids[r].n), hs[r]}; break; }
    }
    if (PROF) fprintf(stderr, "links %zu ids %zu\n", np, ids.size());
    lap("links done");
  };
  auto resolve = [&](u64 h, const char* p, u32 n) -> Person {
    for (u64 j = h & pmask;; j = (j + 1) & pmask) {
      const PSlot& e = ptab[j];
      if (!e.used) return {p, n, h};
      if (e.h == h && e.n == n) return {ref_p(e.pref), ref_n(e.pref), e.ph};  // 64-bit hash + length (see SOLUTION.md)
    }
  };

  // Shard count (power of two): enough that a dedup/session shard stays cache-sized, few enough that each
  // parse thread's per-shard output lists stay in L2. Measured: 512 is best at 5M events; at the scorer's
  // 20M+ a shard would be 4x larger, so use 1024 there (2048 is slower at any size).
  size_t ev_bytes_all = 0;
  for (auto& f : event_files) ev_bytes_all += f.n;
  const int S = getenv("SS_SHARDS") ? atoi(getenv("SS_SHARDS")) : (ev_bytes_all / 90 > (size_t)10000000 ? 1024 : 512);
  const u64 SM = (u64)S - 1;
  struct CRaw { u64 uh; const char* cid; u32 ts; u32 camp; uint16_t uid_n, cid_n; u32 pad; };  // 32 bytes
  vector<BList<CRaw>> cr_buf(NT);
  vector<vector<BList<ERaw>>> er_buf(NT, vector<BList<ERaw>>(S));
  size_t CHUNK = getenv("SS_CHUNK") ? (size_t)atol(getenv("SS_CHUNK")) : 0;  // bytes per parse task
  if (CHUNK < 4096) CHUNK = (size_t)256 << 10;  // small enough that a chunk read with pread stays in L2 while it is parsed
  auto ck_chunks = make_chunks(click_files, CHUNK);
  auto ev_chunks = make_chunks(event_files, CHUNK);
  {
    size_t nck = ck_chunks.size();
    parallel_for(1 + nck + ev_chunks.size(), [&](size_t i, int tid) {
      if (i == 0) { do_links(); return; }
      struct Fence { ~Fence() { _mm_sfence(); } } fence;
      i--;
      if (i < nck) {
        auto& out = cr_buf[tid];
        scan_lines<4>(load_chunk(ck_chunks[i]), [&](const SV* f) {
          if (f[0].n == 0 || f[1].n == 0) return;
          i64 ts = parse_ts(f[3].p, f[3].n);
          if (ts == BAD_TS || ts < cws || ts >= we) return;
          u32 camp = camp_idx.find_fast(f[2].p, f[2].n);
          if (camp == KeyIndex::NONE) return;
          if (f[0].n > 0xFFFF || f[1].n > 0xFFFF) die("id longer than 65535 bytes");
          out.push(CRaw{hash_bytes(f[1].p, f[1].n), keep_str(f[0].p, f[0].n), (u32)(ts - TBASE), camp, (uint16_t)f[1].n, (uint16_t)f[0].n, 0});
        });
      } else {
        auto& out = er_buf[tid];
        scan_lines<7>(load_chunk(ev_chunks[i - nck]), [&](const SV* f) {
          if (f[0].n == 0) return;
          if (DEXP == 11) { dsink += f[6].n; return; }
          if (DEXP == 12) { dsink += parse_ts(f[2].p, f[2].n) + parse_ts(f[3].p, f[3].n); return; }
          if (DEXP == 13) { dsink += parse_ts(f[2].p, f[2].n) + parse_ts(f[3].p, f[3].n) + hash_bytes(f[0].p, f[0].n) + hash_bytes(f[1].p, f[1].n) + prod_idx.find(f[5].p, f[5].n); return; }
          ERaw r;
          r.h_eid = hash_bytes(f[0].p, f[0].n);
          {  // independent 16-bit check: raw first/last two bytes and length (h_eid is a strong 64-bit hash)
            const SV& e = f[0];
            u32 a2 = (uint8_t)e.p[0] | ((u32)(uint8_t)e.p[e.n > 1 ? 1 : 0] << 8);
            u32 z2 = (uint8_t)e.p[e.n - 1] | ((u32)(uint8_t)e.p[e.n > 1 ? e.n - 2 : 0] << 8);
            r.chk = (uint16_t)((a2 * 31 + z2) ^ (e.n * 0x9E37u));
          }
          i64 ingest = parse_ts(f[3].p, f[3].n);
          r.uref = 0; r.uh = 0; r.ts = 0; r.pk = 0; r.cat = 0;
          if (ingest == BAD_TS) r.ingest = INT32_MAX;
          else {
            i64 q = floordiv(ingest - ws, 1000);
            r.ingest = (int32_t)std::max<i64>(INT32_MIN, std::min<i64>(INT32_MAX - 1, q));
          }
          do {
            if (f[1].n == 0 || f[5].n == 0 || ingest == BAD_TS) break;
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
            if (ingest - ts > DAY_MS) break;
            if (f[1].n > 0xFFFF) die("id longer than 65535 bytes");
            u32 pc = prod_idx.find_fast(f[5].p, f[5].n);
            r.cat = pc == KeyIndex::NONE ? unknown_cat : (uint16_t)pc;
            // person is resolved after dedup; keep the raw user id here
            r.uref = pack_ref(keep_str(f[1].p, f[1].n), f[1].n); r.uh = hash_bytes(f[1].p, f[1].n);
            r.ts = (u32)(ts - TBASE); r.pk = price | ((u32)type << 24) | (1u << 26);
          } while (0);
          out[(r.h_eid >> 7) & SM].push(r);
        });
      }
    });
  }

  parallel_for(NT, [&](size_t t, int) { for (auto& l : er_buf[t]) l.finish(); cr_buf[t].finish(); });
  lap("parse events+clicks");
  // ---- dedup events per eid shard, resolve persons (events and clicks) -> person shards
  vector<vector<BList<Ev>>> ev_buf(NT, vector<BList<Ev>>(S));
  vector<vector<BList<Ck>>> ck_buf(NT, vector<BList<Ck>>(S));
  vector<vector<u32>> tab_scratch(NT);
  vector<vector<void*>> blk_scratch(NT);
  vector<vector<u32>> occ_scratch(NT);
  vector<vector<u64>> hv_scratch(NT);
  const int PF = 12;  // prefetch distance for person lookups
  vector<vector<void*>> free_lists(NT);
  parallel_for(S + NT, [&](size_t s, int tid) {
    tl_free = &free_lists[tid];
    struct Fence { ~Fence() { _mm_sfence(); } } fence;
    if (s >= (size_t)S) {  // clicks of one parse thread
      auto& in = cr_buf[s - S];
      auto& out = ck_buf[tid];
      for (auto* b = in.head; b; b = b->next) {
        for (u32 k = 0; k < b->n; k++) {
          if (k + PF < b->n) __builtin_prefetch(&ptab[b->items()[k + PF].uh & pmask]);
          const CRaw& c = b->items()[k];
          Person pp = resolve(c.uh, nullptr, c.uid_n);
          out[pp.h & SM].push(Ck{pp.h, c.cid, c.ts, c.camp, c.cid_n, (uint16_t)pp.n, 0});
        }
      }
      in.recycle();
      return;
    }
    size_t n = 0;
    for (int t = 0; t < NT; t++) n += er_buf[t][s].total;
    if (!n) return;
    // index the shard's copies in place: idx = block number * CAP + item (no copy of the records)
    using EL = BList<ERaw>;
    auto& blks = blk_scratch[tid];
    blks.clear();
    for (int t = 0; t < NT; t++)
      for (auto* b = er_buf[t][s].head; b; b = b->next) blks.push_back(b);
    auto at = [&](u32 idx) -> const ERaw& { return ((const EL::Blk*)blks[idx / EL::CAP])->items()[idx % EL::CAP]; };
    size_t cap = 16; while (cap < n * 2) cap <<= 1;
    auto& tab = tab_scratch[tid];
    tab.assign(cap, UINT32_MAX);
    auto& occ = occ_scratch[tid];
    occ.clear();
    const u64 mask = cap - 1;
    for (u32 bi = 0; bi < (u32)blks.size(); bi++) {
      const EL::Blk* b = (const EL::Blk*)blks[bi];
      const ERaw* it = b->items();
      for (u32 k = 0; k < b->n; k++) {
        if (k + 26 < b->n) _mm_prefetch((const char*)(it + k + 26), _MM_HINT_T0);  // ~1 KB ahead
        const ERaw& r = it[k];
        const u32 i = bi * EL::CAP + k;
        for (u64 j = (r.h_eid >> 20) & mask;; j = (j + 1) & mask) {
          u32& slot = tab[j];
          if (slot == UINT32_MAX) { slot = i; occ.push_back((u32)j); break; }
          const ERaw& o = at(slot);
          if (o.h_eid == r.h_eid && o.chk == r.chk) {
            if (r.ingest < o.ingest) slot = i;
            break;
          }
        }
      }
    }
    size_t w = 0;
    for (u32 j : occ) { u32 x = tab[j]; if (at(x).pk >> 26) occ[w++] = x; }  // winners that are valid
    auto& out = ev_buf[tid];
    // user-id hashes are computed here, only for winning copies (the id bytes are prefetched ahead)
    auto& hv = hv_scratch[tid];
    hv.resize(w);
    for (size_t k = 0; k < w; k++) hv[k] = at(occ[k]).uh;
    for (size_t k = 0; k < w; k++) {
      if (k + PF < w) __builtin_prefetch(&ptab[hv[k + PF] & pmask]);
      const ERaw* r = &at(occ[k]);
      Person pp = resolve(hv[k], ref_p(r->uref), ref_n(r->uref));
      out[pp.h & SM].push(Ev{pp.h, pack_ref(pp.p, pp.n), r->ts, r->pk & 0xFFFFFF, r->cat, (uint8_t)((r->pk >> 24) & 3), 0});
    }
    for (int t = 0; t < NT; t++) er_buf[t][s].recycle();  // blocks go back to this thread for later Ev pushes
  });
  for (auto& f : free_lists) vector<void*>().swap(f);
  parallel_for(NT, [&](size_t t, int) { for (auto& l : ev_buf[t]) l.finish(); for (auto& l : ck_buf[t]) l.finish(); });
  vector<vector<u32>>().swap(tab_scratch);
  vector<vector<void*>>().swap(blk_scratch);
  vector<vector<u32>>().swap(occ_scratch);

  lap("dedup");
  // ---- per person shard: sort, sessionize, attribute, aggregate
  const size_t NCAT = cat_names.size();
  struct CampAgg { vector<i64> sess, purch, rev, assist; };
  vector<CampAgg> cagg(NT);
  for (auto& a : cagg) { size_t z = (size_t)(NC + 1) * ndays; a.sess.assign(z, 0); a.purch.assign(z, 0); a.rev.assign(z, 0); a.assist.assign(z, 0); }
  struct URow { const char* name; u32 n; int32_t day; i64 sessions, events, rev; int32_t top; };
  vector<BList<URow>> urows(NT);  // in recycled event/click blocks: no fresh memory
  vector<vector<void*>> free2(NT);
  struct PKey { u64 h; u32 n; u32 id; };
  struct Scratch {
    vector<PKey> ptab; vector<u32> epid, cpid, cnt, ccnt, eoff, coff, fill;
    vector<Ev> ev; vector<Ck> ck;
    vector<i64> catsum; vector<int> camp_cnt, active_pos; vector<u32> active;
  };
  vector<Scratch> scratch(NT);

  parallel_for(S, [&](size_t s, int tid) {
    tl_free = &free2[tid];
    struct Fence { ~Fence() { _mm_sfence(); } } fence;
    // Group this shard's records by person with a small hash table + counting sort, then sort each
    // person's few events by ts. Person identity = (64-bit name hash, name length): comparing bytes
    // would touch a random input line per event (see SOLUTION.md for the collision bound).
    size_t ne = 0, nc = 0;
    for (int t = 0; t < NT; t++) { ne += ev_buf[t][s].total; nc += ck_buf[t][s].total; }
    if (!ne) return;
    Scratch& sc = scratch[tid];
    // persons per shard are far fewer than events: start small, double at half load
    size_t cap = 64; while (cap < ne / 8) cap <<= 1;
    u64 m = cap - 1;
    sc.ptab.assign(cap, PKey{0, 0, UINT32_MAX});
    sc.epid.resize(ne); sc.cnt.clear();
    auto pfind = [&](u64 h, u32 n, bool insert) -> u32 {
      for (u64 j = (h >> 24) & m;; j = (j + 1) & m) {
        PKey& e = sc.ptab[j];
        if (e.id == UINT32_MAX) {
          if (!insert) return UINT32_MAX;
          u32 id = (u32)sc.cnt.size();
          e = PKey{h, n, id}; sc.cnt.push_back(0);
          if (2 * sc.cnt.size() > cap) {  // grow and rehash
            vector<PKey> old; old.swap(sc.ptab);
            cap <<= 1; m = cap - 1;
            sc.ptab.assign(cap, PKey{0, 0, UINT32_MAX});
            for (const PKey& o : old)
              if (o.id != UINT32_MAX)
                for (u64 k = (o.h >> 24) & m;; k = (k + 1) & m)
                  if (sc.ptab[k].id == UINT32_MAX) { sc.ptab[k] = o; break; }
          }
          return id;
        }
        if (e.h == h && e.n == n) return e.id;
      }
    };
    size_t q = 0;
    for (int t = 0; t < NT; t++) ev_buf[t][s].each([&](const Ev& x) { u32 p = pfind(x.ph, ref_n(x.nref), true); sc.epid[q++] = p; sc.cnt[p]++; });
    const u32 P = (u32)sc.cnt.size();
    sc.eoff.resize(P + 1); sc.eoff[0] = 0;
    for (u32 p = 0; p < P; p++) sc.eoff[p + 1] = sc.eoff[p] + sc.cnt[p];
    sc.fill.assign(sc.eoff.begin(), sc.eoff.end() - 1);
    sc.ev.resize(ne);
    q = 0;
    for (int t = 0; t < NT; t++) ev_buf[t][s].each([&](const Ev& x) { sc.ev[sc.fill[sc.epid[q++]]++] = x; });
    // clicks of persons that have events (others cannot credit or assist anything)
    sc.cpid.resize(nc);
    sc.ccnt.assign(P, 0);
    q = 0;
    for (int t = 0; t < NT; t++) ck_buf[t][s].each([&](const Ck& x) { u32 p = pfind(x.ph, x.name_n, false); sc.cpid[q++] = p; if (p != UINT32_MAX) sc.ccnt[p]++; });
    sc.coff.resize(P + 1); sc.coff[0] = 0;
    for (u32 p = 0; p < P; p++) sc.coff[p + 1] = sc.coff[p] + sc.ccnt[p];
    sc.fill.assign(sc.coff.begin(), sc.coff.end() - 1);
    sc.ck.resize(sc.coff[P]);
    q = 0;
    for (int t = 0; t < NT; t++) ck_buf[t][s].each([&](const Ck& x) { u32 p = sc.cpid[q++]; if (p != UINT32_MAX) sc.ck[sc.fill[p]++] = x; });
    for (int t = 0; t < NT; t++) { ev_buf[t][s].recycle(); ck_buf[t][s].recycle(); }  // copied out: reuse for output rows
    vector<Ev>& ev = sc.ev; vector<Ck>& ck = sc.ck;
    auto ev_less = [](const Ev& a, const Ev& b) { return a.ts < b.ts; };
    auto ck_less = [](const Ck& a, const Ck& b) { return a.ts != b.ts ? a.ts < b.ts : bytecmp(a.cid, a.cid_n, b.cid, b.cid_n) < 0; };
    for (u32 p = 0; p < P; p++) {
      Ev* e0 = ev.data() + sc.eoff[p]; Ev* e1 = ev.data() + sc.eoff[p + 1];
      if (e1 - e0 > 1) {
        if (e1 - e0 <= 32) { for (Ev* x = e0 + 1; x < e1; x++) { Ev v = *x; Ev* y = x; while (y > e0 && v.ts < (y - 1)->ts) { *y = *(y - 1); y--; } *y = v; } }
        else std::sort(e0, e1, ev_less);
      }
      Ck* c0 = ck.data() + sc.coff[p]; Ck* c1 = ck.data() + sc.coff[p + 1];
      if (c1 - c0 > 1) std::sort(c0, c1, ck_less);
    }
    CampAgg& ca = cagg[tid];
    auto& ur = urows[tid];
    vector<i64>& catsum = sc.catsum; catsum.assign(NCAT, 0);
    vector<int>& camp_cnt = sc.camp_cnt; camp_cnt.assign(NC, 0);
    vector<u32>& active = sc.active; active.clear();
    vector<int>& active_pos = sc.active_pos; active_pos.assign(NC, -1);
    for (u32 p = 0; p < P; p++) {
      const size_t i = sc.eoff[p], j = sc.eoff[p + 1];
      const size_t c0 = sc.coff[p], c1 = sc.coff[p + 1];
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
        ur.push(URow{ref_p(ev[i].nref), ref_n(ev[i].nref), cur_day, u_sess, u_ev, u_rev, top});
      };
      size_t k = i;
      while (k < j) {
        i64 start = (i64)ev[k].ts;  // ms since TBASE
        size_t m = k + 1;
        while (m < j && (i64)ev[m].ts - (i64)ev[m - 1].ts <= GAP_MS) m++;
        // session [k,m)
        i64 srev = 0; bool spurch = false;
        for (size_t q = k; q < m; q++)
          if (ev[q].type == 2) { srev += ev[q].price; spurch = true; catsum[ev[q].cat] += ev[q].price; }
        int32_t day = (int32_t)floordiv(start + TBASE, DAY_MS);
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
        while (hi < c1 && (i64)ck[hi].ts <= start) {
          u32 cp = ck[hi].camp;
          if (camp_cnt[cp]++ == 0) { active_pos[cp] = (int)active.size(); active.push_back(cp); }
          hi++;
        }
        while (lo < hi && (i64)ck[lo].ts < start - WEEK_MS) {
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
          u32 t = ck[a].ts;
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
    }
  });

  parallel_for(NT, [&](size_t t, int) { urows[t].finish(); });
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
    if (rs.total == 0 && t != 0) return;
    vector<Col> cols = {{"user_id", C_STR, {}, {}}, {"day", C_DATE, {}, {}}, {"sessions", C_I64, {}, {}},
                        {"events", C_I64, {}, {}}, {"revenue_cents", C_I64, {}, {}}, {"top_category", C_OPTSTR, {}, {}}};
    // exact sizes first, then fill through raw pointers (no per-value vector appends)
    const size_t nr = rs.total;
    size_t nb = 0, cb = 0;
    rs.each([&](const URow& r) { nb += 4 + r.n; if (r.top >= 0) cb += 4 + cat_names[r.top].size(); });
    cols[0].data.b.resize(nb); cols[1].data.b.resize(nr * 4);
    for (int k = 2; k < 5; k++) cols[k].data.b.resize(nr * 8);
    cols[5].data.b.resize(cb); cols[5].def.resize(nr);
    uint8_t* pn = cols[0].data.b.data(); uint8_t* pd = cols[1].data.b.data();
    uint8_t* ps = cols[2].data.b.data(); uint8_t* pe = cols[3].data.b.data(); uint8_t* pr = cols[4].data.b.data();
    uint8_t* pc = cols[5].data.b.data(); uint8_t* pdef = cols[5].def.data();
    size_t i = 0;
    rs.each([&](const URow& r) {
      memcpy(pn, &r.n, 4); memcpy(pn + 4, r.name, r.n); pn += 4 + r.n;
      memcpy(pd + 4 * i, &r.day, 4);
      memcpy(ps + 8 * i, &r.sessions, 8); memcpy(pe + 8 * i, &r.events, 8); memcpy(pr + 8 * i, &r.rev, 8);
      if (r.top >= 0) {
        const string& c = cat_names[r.top]; u32 cl = (u32)c.size();
        memcpy(pc, &cl, 4); memcpy(pc + 4, c.data(), cl); pc += 4 + cl; pdef[i] = 1;
      } else pdef[i] = 0;
      i++;
    });
    write_parquet(ud + "/part-" + std::to_string(t) + ".parquet", cols, (i64)nr);
  });
  cw.join();
  lap("write");
  sample_stop();
  fflush(stdout);
  if (!getenv("SS_NOFREE")) {
    // Tearing down ~GBs of mappings at exit runs on one core; release them in parallel first.
    size_t used = std::min(g_arena_off.load(), g_arena_cap);
    const size_t piece = 64 << 20;
    vector<std::pair<char*, size_t>> parts;
    for (size_t o = 0; o < used; o += piece) parts.push_back({g_arena + o, std::min(piece, used - o)});
    for (auto* v : {&event_files, &click_files, &link_files})
      for (auto& f : *v) if (f.n && f.p) parts.push_back({(char*)f.p, f.n});
    parallel_for(parts.size(), [&](size_t i, int) {
      if (parts[i].first >= g_arena && parts[i].first < g_arena + g_arena_cap) madvise(parts[i].first, parts[i].second, MADV_DONTNEED);
      else munmap(parts[i].first, parts[i].second);
    });
    lap("released");
  }
  if (g_before_exit) g_before_exit();
  _exit(0);  // skip destructors / munmap
}
