import asyncio
import functools
import sys
import functools

def memory_profiler(func):
    @functools.wraps(func)
    async def async_wrapper(*args, **kwargs):
        import gc
        gc.collect()
        mem_before = sum(sys.getsizeof(o) for o in gc.get_objects())
        result = await func(*args, **kwargs)
        gc.collect()
        mem_after = sum(sys.getsizeof(o) for o in gc.get_objects())
        print(f"[MEMORY PROFILER] {func.__name__} growth: {mem_after - mem_before} bytes")
        return result

    @functools.wraps(func)
    def sync_wrapper(*args, **kwargs):
        import gc
        gc.collect()
        mem_before = sum(sys.getsizeof(o) for o in gc.get_objects())
        result = func(*args, **kwargs)
        gc.collect()
        mem_after = sum(sys.getsizeof(o) for o in gc.get_objects())
        print(f"[MEMORY PROFILER] {func.__name__} growth: {mem_after - mem_before} bytes")
        return result

    if asyncio.iscoroutinefunction(func):
        return async_wrapper
    else:
        return sync_wrapper