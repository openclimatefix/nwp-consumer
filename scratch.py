import dask.bag

if __name__ == "__main__":
    bag = dask.bag.from_sequence(range(10))
    output = bag.map(lambda x: (x+1, x-1)).flatten().compute()
    print(output)