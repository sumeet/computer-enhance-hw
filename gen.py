import json
import random

NUM_DISTANCES = 10_000_000
NUM_DISTANCES = 100_000

XRANGE = (-90, 90)
YRANGE = (-180, 180)

def rand_pt():
    return random.uniform(*XRANGE), random.uniform(*YRANGE)

f = open(f'data_{NUM_DISTANCES}_flex.json', 'w')
f.write('{"pairs": [')
for _ in range(NUM_DISTANCES):
    (x0, y0), (x1, y1) = rand_pt(), rand_pt()
    f.write(f'{{"x0": {x0}, "x1": {x1}, "y0": {y0}, "y1": {y1}}},')
f.seek(f.tell()-1)
f.write(']}')
f.close()
