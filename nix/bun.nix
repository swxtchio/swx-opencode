{
  bun,
  fetchurl,
  lib,
}:

let
  packageManager = (builtins.fromJSON (builtins.readFile ../package.json)).packageManager;
  version = lib.removePrefix "bun@" packageManager;
in
bun.overrideAttrs (old: {
  inherit version;
  passthru = old.passthru // {
    sources = {
      "aarch64-darwin" = fetchurl {
        url = "https://github.com/oven-sh/bun/releases/download/bun-v${version}/bun-darwin-aarch64.zip";
        hash = "sha256-wkDX8My5DCY6hJ/NsJt+5iYw2SE0jtaq/ahrnVd7xRA=";
      };
      "aarch64-linux" = fetchurl {
        url = "https://github.com/oven-sh/bun/releases/download/bun-v${version}/bun-linux-aarch64.zip";
        hash = "sha256-uqa7NvUdblhWHgK76oJQOKnfRU9nPW13bou/hvjHhQ4=";
      };
      "x86_64-darwin" = fetchurl {
        url = "https://github.com/oven-sh/bun/releases/download/bun-v${version}/bun-darwin-x64-baseline.zip";
        hash = "sha256-1KFVrfv9CRVes7oFyeOlRPOSl9emhhbTx3WfZ6tcR9w=";
      };
      "x86_64-linux" = fetchurl {
        url = "https://github.com/oven-sh/bun/releases/download/bun-v${version}/bun-linux-x64-baseline.zip";
        hash = "sha256-4ERutVmVdy65iUvrrppr42jZhai5Jx7hJEhvmUbu3XY=";
      };
    };
  };
})
