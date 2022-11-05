#!/bin/env python3

import velox
with open('plan.json', 'r') as f:
    velox.from_json(f.read())
