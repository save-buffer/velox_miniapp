#!/bin/env python3

import velox
import pyarrow as pa

with open('plan.json', 'r') as f:
    result = velox.from_json(f.read())

for vec in result:
    for row in vec:
        print(row)
    rb = vec.to_arrow()
    print(rb)

