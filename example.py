#!/bin/env python3

import velox
import pyarrow as pa
import pyarrow.compute as pc

with open('plan.json', 'r') as f:
    result = velox.from_json(f.read())

for vec in result:
    rb = vec.to_arrow()
    print(rb)
    print('Column 1 has ', pc.count_distinct(rb.column(1).dictionary_decode()), ' distinct values')
