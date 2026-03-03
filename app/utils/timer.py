import time

def timer(func):
    """A decorator to measure the execution time of a function."""
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        elapsed = time.perf_counter() - start
        print(f"Execution time for {func.__name__}: {elapsed:.6f} seconds")
        return result
    return wrapper