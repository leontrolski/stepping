# How transitive closures are implemented in `stepping`

`_transitive_closure` is defined as a `function(a: ZSet) -> ZSet`:

```python
delayed: ZSet[Pair[TIndexable, TIndexable]]
joined = join_lifted(a, delayed, on_left=with_right, on_right=with_left)
picked_outers = linear.map(joined, f=pick_outers)
unioned = linear.add(a, picked_outers)
distincted = distinct_lifted(unioned)
delayed = linear.delay(distincted)
return distincted
```

When we call `st.transitive_closure(a)`, we sum up the ZSet outputs:

```python
_transitive_closure(a) + _transitive_closure(zero) + _transitive_closure(zero) + ...
```

Until we get no new changes. We use the same "store" for the delay values across iterations and subiterations.

`join_lifted` integrates the `l` and `r` inputs into `l_integrated` and `r_integrated_delayed` respectively. It returns the union of `join(l_integrated, r)` and `join(l, r_integrated_delayed)`.


# Walkthrough

If we walk through inserting some example inputs, we can demonstrate (I'm _somewhat_ sure) that the query is incremental - all the joins only try and join on the small input value:

## Iteration 1

```
input
0,4
2,3
0,1

a    delayed  l_integrated  r_integrated_delayed
0,4  ()       0,4           ()
2,3           2,3
0,1           0,1

a    delayed  l_integrated  r_integrated_delayed
()   0,4      0,4           ()
     2,3      2,3
     0,1      0,1

# After both subiterations, nothing gets joined and we just return the input.

output
0,4
2,3
0,1
```


## Iteration 2

```
input
1,2

a    delayed  l_integrated  r_integrated_delayed
1,2  ()       0,4           0,4
              2,3           2,3
              1,2           0,1
              0,1

# After the first subiteration
# `a=1,2` joins to `r_integrated_delayed=2,3`, meaning we get `1,2` in `picked_outers`

a    delayed  l_integrated  r_integrated_delayed
()   1,3      0,4           0,4
     1,2      2,3           2,3
              1,2           0,1
              0,1

# After the second subiteration
# `l_integrated=0,1` joins to `delayed=1,2` meaning we get `0,2` in `picked_outers`
# `l_integrated=0,1` joins to `delayed=1,3` meaning we get `0,3` in `picked_outers`

a    delayed  l_integrated  r_integrated_delayed
()   0,2      0,4           0,4
     0,3      2,3           2,3
              1,2           0,1
              0,1           1,2
                            1,3

# After the third subiteration, nothing more gets joined, we return the union of the join and the input.

output
1,2
0,2
0,3
1,3
```

## Iteration 3

```
input
0,4 [remove]

a             delayed       l_integrated  r_integrated_delayed
0,4 [remove]  ()            2,3           1,3
                            1,2           0,2
                            0,1           0,4
                                          2,3
                                          1,2
                                          0,3
                                          0,1

a             delayed       l_integrated  r_integrated_delayed
()            0,4 [remove]  2,3           1,3
                            1,2           0,2
                            0,1           0,4
                                          2,3
                                          1,2
                                          0,3
                                          0,1

# After both subiterations, nothing gets joined and we just return the input.

output
0,4 [remove]
```
