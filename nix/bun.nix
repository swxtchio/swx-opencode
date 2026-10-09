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
        hash = "sha256-kJh6OhbX21VtiGrD1VHnttPt8KHPQ6yu1iLoZ2vh0S8=";
      };
      "aarch64-linux" = fetchurl {
        url = "https://github.com/oven-sh/bun/releases/download/bun-v${version}/bun-linux-aarch64.zip";
        hash = "sha256-VDKLvC2cjgyfiSxUTWbFeoO4QTnjSQnl7oF1jxrI/ac=";
      };
      "x86_64-darwin" = fetchurl {
        url = "https://github.com/oven-sh/bun/releases/download/bun-v${version}/bun-darwin-x64-baseline.zip";
        hash = "sha256-utW71s8U0JgNEV9ZVMn/kE32GdXplNLaH/zNPzFjALA=";
      };
      "x86_64-linux" = fetchurl {
        url = "https://github.com/oven-sh/bun/releases/download/bun-v${version}/bun-linux-x64-baseline.zip";
        hash = "sha256-xngEDxT+BEDrg503y9DOTAUaMtpygGrJfeamqra/co8=";
      };
    };
  };
})
