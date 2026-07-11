{
  description = "vrcwscrape / mapgen dev environment (NixOS-friendly; FHS for uv wheels)";

  # Track nixos-unstable to match the dev hosts' channel, so the FHS runtime
  # closure is already in their nix store.
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs =
    { self, nixpkgs }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
      ];
      forAllSystems = f: nixpkgs.lib.genAttrs systems (system: f nixpkgs.legacyPackages.${system});

      # NixOS has no /usr/lib or standard ELF loader, so uv's downloaded CPython
      # and manylinux wheels (hdbscan, llvmlite/numba, pyarrow, duckdb, ...)
      # can't load or import. buildFHSEnv provides a normal FHS userland where
      # `uv sync` / `uv run` just work — for both the root scraper and mapgen.
      mkFhs =
        pkgs:
        pkgs.buildFHSEnv {
          name = "vrcwscrape-dev";
          targetPkgs =
            p: with p; [
              uv
              git
              nodejs_22 # web/ frontend (vite + deck.gl + duckdb-wasm)

              # runtime libs the scientific manylinux wheels dynamically link
              zlib
              stdenv.cc.cc.lib # libstdc++ / libgcc_s
              xz # liblzma
              openssl

              # NOTE: dolt is intentionally omitted. The 14G Dolt DB lives on
              # chirashi; the Stage-A ETL reaches its dolt-sql-server over the
              # LAN via pymysql, e.g.
              #   DATABASE_URL='mysql://root@chirashi:13306/vrcwscrape'
              # No local dolt binary or DB copy is needed for dev here.
            ];
          runScript = "bash";
        };
    in
    {
      # `nix develop` can't chroot into buildFHSEnv's `.env` (that attr is a
      # nix-shell mechanism), so enter a plain shell and immediately exec the
      # FHS wrapper — this lands you inside the FHS userland with uv/node/git
      # on PATH.
      devShells = forAllSystems (pkgs: {
        default = pkgs.mkShell {
          shellHook = "exec ${mkFhs pkgs}/bin/vrcwscrape-dev";
        };
      });

      # `nix run` / the built wrapper enter the same FHS non-interactively:
      #   nix run .#            -> interactive FHS bash
      #   nix run .# -- -c 'uv sync && uv run mapgen-render ...'
      # This also gives a scriptable entrypoint that doesn't depend on the
      # interactive-shell semantics `nix develop` relies on.
      packages = forAllSystems (pkgs: rec {
        dev = mkFhs pkgs;
        default = dev;
      });

      apps = forAllSystems (pkgs: rec {
        dev = {
          type = "app";
          program = "${mkFhs pkgs}/bin/vrcwscrape-dev";
        };
        default = dev;
      });
    };
}
