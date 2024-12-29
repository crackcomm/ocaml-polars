{
  description = "Nix Flake";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    ocaml-overlay.url = "github:anmonteiro/nix-overlays";
    nix-filter.url = "github:numtide/nix-filter";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay.url = "github:oxalica/rust-overlay";
  };

  outputs =
    { self, nixpkgs, nix-filter, flake-utils, ocaml-overlay, rust-overlay }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [
          rust-overlay.overlays.default
          ocaml-overlay.overlays.default
          (final: prev: {
            rustToolchain =
              prev.rust-bin.nightly."2024-11-28".default.override {
                targets = [ "x86_64-unknown-linux-gnu" ];
              };
          })
        ];
        pkgs = (import nixpkgs { inherit system overlays; }).extend
          (self: super: { ocamlPackages = super.ocaml-ng.ocamlPackages_5_2; });
        ocaml-polars = pkgs.callPackage ./nix { inherit pkgs nix-filter; };
      in {
        packages = { inherit ocaml-polars; };
        devShell = import ./nix/shell.nix { inherit pkgs ocaml-polars; };
      });
}
