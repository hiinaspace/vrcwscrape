# FHS environment for running uv + manylinux Python wheels on NixOS.
#
# NixOS has no standard /usr/lib or loader, so uv's downloaded CPython and
# wheels with C/C++ extensions (hdbscan, llvmlite/numba, pyarrow) won't run /
# import. buildFHSEnv gives a normal FHS userland where everything just works.
#
# This returns the *runnable* FHS wrapper (its bin/mapgen-fhs enters the env):
#   FHS=$(nix-build --no-out-link fhs.nix)
#   $FHS/bin/mapgen-fhs -c 'uv sync && uv run mapgen-embed ...'   # scripted
#   $FHS/bin/mapgen-fhs                                            # interactive
# For `nix-shell` users, shell.nix exposes the matching .env.
{ pkgs ? import <nixpkgs> { } }:

pkgs.buildFHSEnv {
  name = "mapgen-fhs";
  targetPkgs = pkgs: with pkgs; [
    uv
    # runtime libs that scientific manylinux wheels dynamically link against
    zlib
    stdenv.cc.cc.lib # libstdc++ / libgcc_s
    xz # liblzma
    openssl
  ];
  runScript = "bash";
}
