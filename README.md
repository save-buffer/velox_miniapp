# velox_miniapp
A quick and dirty way to link to Velox and experiment with it!

Currently, there's one program here: from_substrait. It takes in as an argument a path to a protobuf file containing a substrait
plan and attempts to run it using Velox. There are also python bindings that run substrait from Python using Velox. 

# Usage
1. Follow the instructions on Velox's homepage to compile it (using the Makefile). Make sure that Substrait and Parquet are enabled!
2. By default, the Makefile in this repository assumes that Velox's root directory is at ~/velox. If you downloaded it elsewhere, be sure to set the environment variable VELOX_ROOT to the appropriate directory.
3. Run `make`
4. Run `./from_substrait plan.json` and `./from_substrait plan_simple.json`! The program outputs via stdout, so the output can be long. You can do `./from_substrait plan.json > results.txt` to get them in a file.

# Python Bindings
1. After running `make`, there should be a file `velox.so`.
2. If you run `python` from the repository directory, you should be able to `import python`
3. Take your plan's json and feed it to `velox.from_json`!

```python
import velox
import pyarrow as pa

# Read the plan's substrait json and feed it to Velox
# It returns a VeloxResult
with open('plan.json', 'r') as f:
    result = velox.from_json(f.read())

# Iterate through the result to get VeloxVectors
for vec in result:
    # VeloxVector's are convertible to pyarrow record batches!
    rb = vec.to_arrow()
```