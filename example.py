#!/bin/env python3

import velox

with open('plan.json', 'r') as f:
    result = velox.from_json(f.read())

for vec in result:
    rb = vec.to_arrow()
    print(rb)
