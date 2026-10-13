{
  inputs = {
    gen-harness.url = "github:sini/gen-harness";
    # ★ THE SUBJECT IS READ BY RELATIVE PATH, NEVER AS A `path:..` INPUT. Lix refuses a relative
    # `path` node in a lock ("mutable lock"), and a Lix-written `path:..?narHash=…` lock pins a stale
    # snapshot of the tree (den-hoag-lbtnv D1). So `outputs` below applies `../flake.nix`'s own
    # `outputs` to the `nixpkgs` declared here, line for line as `../flake.nix` declares it.
    nixpkgs.url = "https://channels.nixos.org/nixos-unstable/nixexprs.tar.xz";
    # The flake modules are evaluated against home-manager at nix-config's pin (its
    # `home-manager-unstable` node), so a module change that nix-config cannot take reds here.
    home-manager = {
      url = "github:nix-community/home-manager/fae6e9e42c3b762ab47635cddcfaf6f52374a61b";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    inputs@{ gen-harness, nixpkgs, ... }:
    let
      # The published surface of THIS tree.
      xmsg = (import ../flake.nix).outputs {
        inherit (inputs) self;
        inherit nixpkgs;
      };
    in
    gen-harness.lib.mkCi {
      inputs = inputs // {
        inherit xmsg;
      };
      name = "xmsg";
      testModules = ./tests;
      extraModules = [
        # xmsg is a TOOL, not an ecosystem library: it is absent from the register roster,
        # so no capability sheet is owed.
        { gen.ci.agentsMd.sheet = "not-owed"; }
        # Nor a root library surface: there is no root default.nix.
        { gen.ci.rootSurface.entry = "not-owed"; }
        # Nor an evaluator matrix: its CI is one upstream-Nix column, not evaluators.yml.
        { gen.ci.evaluators = "not-owed"; }
        # Surface package build in CI checks (doCheck = true runs cargo test)
        {
          perSystem =
            { pkgs, system, ... }:
            {
              checks.package = xmsg.packages.${system}.default;
            };
        }
      ];
    };
}
