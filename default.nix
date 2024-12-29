{ pkgs ? import <nixpkgs>, nix-filter }:
pkgs.callPackage ./nix/default.nix { inherit pkgs nix-filter; }
