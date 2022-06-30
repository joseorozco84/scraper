from memory_profiler import profile

@profile
def fibo(numero):
    a, b = 0, 1
    for _ in range(numero):
        yield a
        a, b = b, a + b

if __name__ == "__main__":
    print(list(fibo(10)))   