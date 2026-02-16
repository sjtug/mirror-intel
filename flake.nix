{
  description = "A Nix-flake-based Rust development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-parts = {
      url = "github:hercules-ci/flake-parts";
      inputs.nixpkgs-lib.follows = "nixpkgs";
    };
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    crane.url = "github:ipetkov/crane"; # Check https://crane.dev for detailed guide
    advisory-db = {
      url = "github:rustsec/advisory-db";
      flake = false;
    };
    pre-commit-hooks = {
      url = "github:cachix/git-hooks.nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    treefmt-nix = {
      url = "github:numtide/treefmt-nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    inputs@{ flake-parts, advisory-db, ... }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      imports = [
        inputs.treefmt-nix.flakeModule
        inputs.pre-commit-hooks.flakeModule
      ];

      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "aarch64-darwin"
        "x86_64-darwin"
      ];

      perSystem =
        {
          config,
          pkgs,
          lib,
          system,
          ...
        }:
        let
          craneLib = inputs.crane.mkLib pkgs;
          craneAttrs = import ./nix/crane.nix { inherit craneLib pkgs lib; };

          inherit (craneAttrs)
            commonArgs
            cargoArtifacts
            src
            my-crate
            ;
        in
        {
          _module.args.pkgs = import inputs.nixpkgs {
            inherit system;
            overlays = [
              inputs.rust-overlay.overlays.default
              (_final: prev: {
                rustToolchain =
                  let
                    rust = prev.rust-bin;
                  in
                  # if builtins.pathExists ./rust-toolchain.toml then
                  #   rust.fromRustupToolchainFile ./rust-toolchain.toml
                  # else if builtins.pathExists ./rust-toolchain then
                  #   rust.fromRustupToolchainFile ./rust-toolchain
                  # else
                  rust.stable.latest.default.override {
                    extensions = [
                      "rust-src"
                      "rust-analyzer"
                    ];
                    # targets = [ "arm-unknown-linux-gnueabihf" ];
                  };
              })
            ];
          };

          # https://flake.parts/options/treefmt-nix.html
          # Example: https://github.com/nix-community/buildbot-nix/blob/main/nix/treefmt/flake-module.nix
          treefmt = {
            projectRootFile = "flake.nix";
            settings.global.excludes = [ ];

            programs = {
              autocorrect.enable = true;
              nixfmt.enable = true;
            };
          };

          # https://flake.parts/options/git-hooks-nix.html
          # Example: https://github.com/cachix/git-hooks.nix/blob/master/template/flake.nix
          pre-commit.settings.package = pkgs.prek;
          pre-commit.settings.configPath = ".pre-commit-config.flake.yaml";
          pre-commit.settings.hooks = {
            commitizen.enable = true;
            eclint.enable = true;
            treefmt.enable = true;
          };

          checks = {
            # Build the crate as part of `nix flake check` for convenience
            default = my-crate;
            inherit my-crate;

            # Run clippy (and deny all warnings) on the crate source,
            # again, reusing the dependency artifacts from above.
            #
            # Note that this is done as a separate derivation so that
            # we can block the CI if there are issues here, but not
            # prevent downstream consumers from building our crate by itself.
            my-crate-clippy = craneLib.cargoClippy (
              commonArgs
              // {
                inherit cargoArtifacts;
                cargoClippyExtraArgs = "--all-targets -- --deny warnings";
              }
            );

            my-crate-doc = craneLib.cargoDoc (
              commonArgs
              // {
                inherit cargoArtifacts;
                # This can be commented out or tweaked as necessary, e.g. set to
                # `--deny rustdoc::broken-intra-doc-links` to only enforce that lint
                env.RUSTDOCFLAGS = "--deny warnings";
              }
            );

            # Check formatting
            my-crate-fmt = craneLib.cargoFmt {
              inherit src;
            };

            # my-crate-toml-fmt = craneLib.taploFmt {
            #   src = lib.sources.sourceFilesBySuffices src [ ".toml" ];
            #   # taplo arguments can be further customized below as needed
            #   # taploExtraArgs = "--config ./taplo.toml";
            # };

            # Audit dependencies
            my-crate-audit = craneLib.cargoAudit {
              inherit src advisory-db;
            };

            # Audit licenses
            my-crate-deny = craneLib.cargoDeny {
              inherit src;
            };

            # Run tests with cargo-nextest
            # Consider setting `doCheck = false` on `my-crate` if you do not want
            # the tests to run twice
            my-crate-nextest = craneLib.cargoNextest (
              commonArgs
              // {
                inherit cargoArtifacts;
                partitions = 1;
                partitionType = "count";
                cargoNextestPartitionsExtraArgs = "--no-tests=pass";
              }
            );
          };

          devShells.default = pkgs.mkShell {
            inputsFrom = [
              config.treefmt.build.devShell
              config.pre-commit.devShell
            ];

            shellHook = ''
              echo 1>&2 "Welcome to the development shell!"
            '';

            nativeBuildInputs = with pkgs; [
              ### Rust toolchain ###
              rustToolchain
              # rustPlatform.bindgenHook

              ### Miscellaneous ###
              # valgrind
              # cargo-bloat
              # cargo-outdated
              # cargo-show-asm
              # samply
              # watchexec
              # bacon
            ];
          };

          packages.default = my-crate;
        };
    };
}
