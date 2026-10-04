### Mapper 0 - split `d1.txt`

- вход: `(0, "the cat sat on the mat")`
- вход: `(23, "the cat ate the fish")`

Выход map() (11 пар): `(the@d1.txt, 1)`, `(cat@d1.txt, 1)`, `(sat@d1.txt, 1)`, `(on@d1.txt, 1)`, `(the@d1.txt, 1)`, `(mat@d1.txt, 1)`, `(the@d1.txt, 1)`, `(cat@d1.txt, 1)`, `(ate@d1.txt, 1)`, `(the@d1.txt, 1)`, `(fish@d1.txt, 1)`

После sort + combiner в кольцевом буфере (7 пар), с номером партиции:

- партиция 0: `(ate@d1.txt, 1)`, `(cat@d1.txt, 2)`, `(fish@d1.txt, 1)`, `(mat@d1.txt, 1)`, `(sat@d1.txt, 1)`
- партиция 1: `(on@d1.txt, 1)`, `(the@d1.txt, 4)`

### Mapper 1 - split `d2.txt`

- вход: `(0, "the dog sat on the log")`

Выход map() (6 пар): `(the@d2.txt, 1)`, `(dog@d2.txt, 1)`, `(sat@d2.txt, 1)`, `(on@d2.txt, 1)`, `(the@d2.txt, 1)`, `(log@d2.txt, 1)`

После sort + combiner в кольцевом буфере (5 пар), с номером партиции:

- партиция 0: `(on@d2.txt, 1)`, `(the@d2.txt, 2)`
- партиция 1: `(dog@d2.txt, 1)`, `(log@d2.txt, 1)`, `(sat@d2.txt, 1)`

### Mapper 2 - split `d3.txt`

- вход: `(0, "cat and dog are friends")`

Выход map() (5 пар): `(cat@d3.txt, 1)`, `(and@d3.txt, 1)`, `(dog@d3.txt, 1)`, `(are@d3.txt, 1)`, `(friends@d3.txt, 1)`

После sort + combiner в кольцевом буфере (5 пар), с номером партиции:

- партиция 0: `(are@d3.txt, 1)`, `(cat@d3.txt, 1)`, `(dog@d3.txt, 1)`
- партиция 1: `(and@d3.txt, 1)`, `(friends@d3.txt, 1)`

### Reduce-сторона: fetch → merge-sort → группировка

**Reducer 0** скачал 10 пар у mapper'ов [0, 1, 2]; после слияния reduce() вызывается для групп:

| ключ | значения (после shuffle) | выход reduce() |
|---|---|---|
| `are@d3.txt` | `[1]` | `(are@d3.txt, 1)` |
| `ate@d1.txt` | `[1]` | `(ate@d1.txt, 1)` |
| `cat@d1.txt` | `[2]` | `(cat@d1.txt, 2)` |
| `cat@d3.txt` | `[1]` | `(cat@d3.txt, 1)` |
| `dog@d3.txt` | `[1]` | `(dog@d3.txt, 1)` |
| `fish@d1.txt` | `[1]` | `(fish@d1.txt, 1)` |
| `mat@d1.txt` | `[1]` | `(mat@d1.txt, 1)` |
| `on@d2.txt` | `[1]` | `(on@d2.txt, 1)` |
| `sat@d1.txt` | `[1]` | `(sat@d1.txt, 1)` |
| `the@d2.txt` | `[2]` | `(the@d2.txt, 2)` |

**Reducer 1** скачал 7 пар у mapper'ов [0, 1, 2]; после слияния reduce() вызывается для групп:

| ключ | значения (после shuffle) | выход reduce() |
|---|---|---|
| `and@d3.txt` | `[1]` | `(and@d3.txt, 1)` |
| `dog@d2.txt` | `[1]` | `(dog@d2.txt, 1)` |
| `friends@d3.txt` | `[1]` | `(friends@d3.txt, 1)` |
| `log@d2.txt` | `[1]` | `(log@d2.txt, 1)` |
| `on@d1.txt` | `[1]` | `(on@d1.txt, 1)` |
| `sat@d2.txt` | `[1]` | `(sat@d2.txt, 1)` |
| `the@d1.txt` | `[4]` | `(the@d1.txt, 4)` |

