'''A function solving the problem of sums square
ie: Can order number from 1 to n so that each consecutive pair sums to a perfect square
This example is a DFS using the yield from syntax'''

def square_sums(n):

    squares = {i**2 for i in range(2, math.ceil((2*n-1)**0.5))}

    def dfs():
        if not inp: yield res
        for v in tuple(inp):
            if not res or((res[-1]+v) in squares):
                res.append(v)
                inp.discard(v)
                yield from dfs()
                inp.add(res.pop())

    inp, res = set(range(1,n+1)), []
    return next(dfs(), False)