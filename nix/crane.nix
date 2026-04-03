{
  craneLib,
  pkgs,
  lib,
  ...
}:
let
  inherit (pkgs) cacert;

  unfilteredRoot = ../.; # TODO: decouple with filepath
  src = lib.fileset.toSource {
    root = unfilteredRoot;
    fileset = lib.fileset.unions [
      (craneLib.fileset.commonCargoSources unfilteredRoot)
      (lib.fileset.maybeMissing (unfilteredRoot + "/tests"))
    ];
  };
  commonArgs = {
    inherit src;
    strictDeps = true;

    nativeBuildInputs = [ cacert ];

    doCheck = false; # Test separately with cargo-nextest
  };

  # Build *just* the cargo dependencies, so we can reuse
  # all of that work (e.g. via cachix) when running in CI
  cargoArtifacts = craneLib.buildDepsOnly commonArgs;

  # Merge crane arg sets while appending common list-valued inputs
  # instead of letting `//` replace them.
  mergeCraneArgs =
    base: extra:
    let
      listMergeKeys = [
        "nativeBuildInputs"
        "buildInputs"
        "propagatedBuildInputs"
        "propagatedNativeBuildInputs"
        "checkInputs"
      ];
      mergedListAttrs = builtins.foldl' (
        acc: key:
        if (builtins.hasAttr key base) || (builtins.hasAttr key extra) then
          acc
          // {
            ${key} = (base.${key} or [ ]) ++ (extra.${key} or [ ]);
          }
        else
          acc
      ) { } listMergeKeys;
    in
    (base // extra) // mergedListAttrs;
in
{
  inherit
    src
    commonArgs
    cargoArtifacts
    mergeCraneArgs
    ;
}
