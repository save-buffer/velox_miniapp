# velox_miniapp
A quick and dirty way to link to Velox and experiment with it!

Currently, there's only one program here: from_substrait. It takes in as an argument a path to a protobuf file containing a substrait
plan and attempts to run it using Velox.

# Usage
1. Follow the instructions on Velox's homepage to compile it (using the Makefile). Make sure that substrait is enabled!
2. By default, the Makefile in this repository assumes that Velox's root directory is at ~/velox. If you downloaded it elsewhere, be sure to set the environment variable VELOX_ROOT to the appropriate directory.
3. Run `make`
4. Profit!