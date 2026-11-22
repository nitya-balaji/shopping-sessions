// Linked only into the instrumented build of a profile-guided compile (see Dockerfile): makes main()
// write the profile before it calls _exit.
extern "C" void __gcov_dump(void);
extern void (*g_before_exit)();
static struct PgoInit { PgoInit() { g_before_exit = __gcov_dump; } } pgo_init;
