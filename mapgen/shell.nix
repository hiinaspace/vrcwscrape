# Interactive FHS shell for NixOS: `nix-shell shell.nix` drops you into the
# FHS userland where uv + Python wheels work. See fhs.nix for the definition
# and for the scripted (non-interactive) invocation pattern.
(import ./fhs.nix { }).env
