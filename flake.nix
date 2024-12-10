{
  description = "Nix Flake";

  inputs = {
    nixpkgs.url = "github:anmonteiro/nix-overlays";
    nix-filter.url = "github:numtide/nix-filter";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay.url = "github:oxalica/rust-overlay";
    # fenix = {
    #   url = "github:nix-community/fenix";
    #   inputs.nixpkgs.follows = "nixpkgs";
    # };
  };

  outputs = { self, nixpkgs, nix-filter, flake-utils, rust-overlay }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = (nixpkgs.makePkgs { inherit system overlays; }).extend
          (self: super: {
            ocamlPackages = import ./nix/ocaml.nix { pkgs = super; };
          });
      in let
        ocaml-polars = pkgs.callPackage ./nix { inherit nix-filter; };
        # in let
        #   # TODO: different platforms
        #   rustToolchain = (fenix.packages.x86_64-linux.toolchainOf {
        #     channel = "nightly";
        #     date = "2024-11-28";
        #     sha256 = "sha256-lmQQppk1opfsDa+37lYNHvOwC5CXgIInS7pAnLoMSKM=";
        #   }).minimalToolchain;
      in {
        packages = { inherit ocaml-polars; };
        devShell = import ./nix/shell.nix { inherit pkgs ocaml-polars; };
      });
}
