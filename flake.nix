{
  description = "OpenCode development flake";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
  };

  outputs =
    { self, nixpkgs, ... }:
    let
      systems = [
        "aarch64-linux"
        "x86_64-linux"
        "aarch64-darwin"
        "x86_64-darwin"
      ];
      forEachSystem = f:
        nixpkgs.lib.genAttrs systems (
          system:
          let
            pkgs = nixpkgs.legacyPackages.${system};
            bun = pkgs.callPackage ./nix/bun.nix { };
          in
          f pkgs bun
        );
      rev = self.shortRev or self.dirtyShortRev or "dirty";
    in
    {
      devShells = forEachSystem (pkgs: bun: {
        default = pkgs.mkShell {
          packages = [ bun pkgs.nodejs_20 pkgs.pkg-config pkgs.openssl pkgs.git ];
        };
      });

      overlays = {
        default =
          final: prev:
          let
            bun = final.callPackage ./nix/bun.nix { bun = prev.bun; };
            node_modules = final.callPackage ./nix/node_modules.nix {
              # This fixed-output derivation only materializes dependencies; binary builds use the fixed Bun.
              bun = prev.bun;
              inherit rev;
            };
            opencode = final.callPackage ./nix/opencode.nix {
              inherit bun node_modules;
            };
          in
          {
            inherit bun opencode;
            opencode-desktop = final.callPackage ./nix/desktop.nix {
              inherit bun opencode;
            };
          };
      };

      packages = forEachSystem (
        pkgs: bun:
        let
          node_modules = pkgs.callPackage ./nix/node_modules.nix {
            # This fixed-output derivation only materializes dependencies; binary builds use the fixed Bun.
            bun = pkgs.bun;
            inherit rev;
          };
          opencode = pkgs.callPackage ./nix/opencode.nix {
            inherit bun node_modules;
          };
        in
        {
          default = opencode;
          inherit opencode;
          opencode-desktop = pkgs.callPackage ./nix/desktop.nix {
            inherit bun opencode;
          };
          # Updater derivation with fakeHash - build fails and reveals correct hash
          node_modules_updater = node_modules.override {
            hash = pkgs.lib.fakeHash;
          };
        }
      );
    };
}
