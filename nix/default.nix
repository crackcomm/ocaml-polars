{ pkgs, nix-filter }:

let inherit (pkgs) lib stdenv ocamlPackages;

in with ocamlPackages;
buildDunePackage rec {
  pname = "polars";
  version = "0.0.0-dev";

  src = with nix-filter.lib;
    filter {
      root = ./..;
      include = [ "dune-project" ];
      exclude = [ ];
    };

  propagatedBuildInputs = [ async core dune-configurator ppx_typed_fields ]
    ++ checkInputs;

  checkInputs = [ expect_test_helpers_async ocaml-index ];
}
