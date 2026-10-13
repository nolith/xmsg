# The flake modules (homeManagerModules.xmsg, nixosModules.xmsg), evaluated against
# home-manager at nix-config's pin. Cells are evaluations; `peers-render` builds.
{ lib, inputs, ... }:
let
  inherit (inputs) xmsg home-manager nixpkgs;

  # home-manager's own evaluation (its modules/default.nix) without the wrapper that throws
  # on a failed assertion, so a cell can read `assertions` as data.
  hmEval =
    system: extra:
    let
      pkgs = nixpkgs.legacyPackages.${system};
      hmLib = import "${home-manager}/modules/lib/stdlib-extended.nix" nixpkgs.lib;
    in
    (hmLib.evalModules {
      class = "homeManager";
      specialArgs.modulesPath = "${home-manager}/modules";
      modules =
        import "${home-manager}/modules/modules.nix" {
          inherit pkgs;
          lib = hmLib;
          check = true;
        }
        ++ [
          xmsg.homeManagerModules.xmsg
          (
            { pkgs, ... }:
            {
              home = {
                username = "alice";
                homeDirectory = if pkgs.stdenv.hostPlatform.isDarwin then "/Users/alice" else "/home/alice";
                stateVersion = "25.11";
                enableNixpkgsReleaseCheck = false;
              };
              programs.claude-code = {
                enable = true;
                package = null;
              };
              # Evaluated, never built: the anchor only needs a store path.
              programs.antigravity-cli = {
                enable = true;
                package = pkgs.runCommand "antigravity-cli-fixture" { } "mkdir -p $out/bin; touch $out/bin/agy";
              };
              programs.xmsg = {
                enable = true;
                harnesses = {
                  claude.enable = true;
                  agy.enable = true;
                  pi.enable = true;
                };
                server = {
                  httpSocket = "/run/user/1000/xmsg/http.sock";
                  svcExe.my-bot = "/opt/my-bot/bin/my-bot";
                  federation = {
                    enable = true;
                    port = 7790;
                    cert = "/run/secrets/xmsg.crt";
                    key = "/run/secrets/xmsg.key";
                    reloadInterval = 60;
                    peers.cortex = {
                      address = "cortex.example.org:7790";
                      pin = "sha256:0000000000000000000000000000000000000000000000000000000000000000";
                      allow = [ "send" ];
                    };
                  };
                };
              };
            }
          )
          extra
        ];
    }).config;

  # The fixture's literal-pin peer, rendered as the loader reads it.
  pinLiteralPeers = nixpkgs.legacyPackages.x86_64-linux.writeText "xmsg-peers.json" (
    builtins.toJSON {
      cortex = {
        address = "cortex.example.org:7790";
        pin = "sha256:0000000000000000000000000000000000000000000000000000000000000000";
        allow = [ "send" ];
      };
    }
  );

  failedAssertions = cfg: map (a: a.message) (lib.filter (a: !a.assertion) cfg.assertions);

  linux = hmEval "x86_64-linux" { };
  exe = lib.getExe xmsg.packages.x86_64-linux.default;
  agyAnchor = "${linux.programs.antigravity-cli.package}/bin/agy";
  file = cfg: name: cfg.home.file.${name};

  darwinCells =
    system:
    let
      c = hmEval system { };
    in
    {
      expr = {
        launchd = c.launchd.agents.xmsg.enable or false;
        launchdArgs = lib.take 2 (c.launchd.agents.xmsg.config.ProgramArguments or [ ]);
        systemd = c.systemd.user.services ? xmsg;
        failed = failedAssertions c;
      };
      expected = {
        launchd = true;
        launchdArgs = [
          (lib.getExe xmsg.packages.${system}.default)
          "serve"
        ];
        systemd = false;
        failed = [ ];
      };
    };

  nixosPorts =
    enable:
    (nixpkgs.lib.nixosSystem {
      system = "x86_64-linux";
      modules = [
        xmsg.nixosModules.xmsg
        {
          programs.xmsg.federation = {
            inherit enable;
            port = 7790;
          };
        }
      ];
    }).config.networking.firewall.allowedTCPPorts;
in
{
  flake.tests.module = {
    # Oracle 1: the Linux unit, each harness's pieces.
    test-linux-unit = {
      expr = {
        exec = linux.systemd.user.services.xmsg.Service.ExecStart;
        launchd = linux.launchd.agents ? xmsg;
        failed = failedAssertions linux;
      };
      expected = {
        exec = [
          "${exe} serve --http-sock /run/user/1000/xmsg/http.sock --agy-exe ${agyAnchor} --svc-exe 'my-bot=/opt/my-bot/bin/my-bot' --fed-listen 0.0.0.0:7790 --fed-cert /run/secrets/xmsg.crt --fed-key /run/secrets/xmsg.key --peers-file ${pinLiteralPeers} --fed-reload-interval-secs 60"
        ];
        launchd = false;
        failed = [ ];
      };
    };
    test-claude = {
      expr = {
        mcp = linux.programs.claude-code.mcpServers.xmsg;
        skill = linux.programs.claude-code.skills.xmsg;
        allow = linux.programs.claude-code.settings.permissions.allow;
      };
      expected = {
        mcp = {
          type = "stdio";
          command = exe;
          args = [ "mcp" ];
          env = { };
        };
        skill = ../../nix/modules/skills/xmsg;
        allow = [
          "mcp__xmsg__list"
          "mcp__xmsg__send"
          "mcp__xmsg__reply"
        ];
      };
    };
    test-agy = {
      expr = {
        mcp = linux.programs.antigravity-cli.mcpServers.xmsg;
        skill = linux.programs.antigravity-cli.skills.xmsg;
        hooks = builtins.fromJSON (
          builtins.unsafeDiscardStringContext (file linux ".gemini/config/hooks.json").text
        );
      };
      expected = {
        mcp = {
          command = exe;
          args = [ "mcp" ];
        };
        skill = ../../nix/modules/skills/xmsg;
        hooks.xmsg-register.PreInvocation = [
          {
            type = "command";
            command = "${exe} register agy --hook";
          }
        ];
      };
    };
    test-pi = {
      expr = {
        extension = builtins.readFile (file linux ".pi/agent/extensions/xmsg-pi.ts").source;
        skill = builtins.readFile (file linux ".pi/agent/skills/xmsg/SKILL.md").source;
      };
      expected = {
        extension = builtins.readFile ../../extensions/pi/index.ts;
        skill = builtins.readFile ../../nix/modules/skills/xmsg/SKILL.md;
      };
    };
    # D19: a switch whose harness module is off is refused, naming the option.
    test-switch-needs-harness = {
      expr = failedAssertions (
        hmEval "x86_64-linux" {
          programs.claude-code.enable = lib.mkForce false;
          programs.antigravity-cli.enable = lib.mkForce false;
        }
      );
      expected = [
        "programs.xmsg.harnesses.claude.enable needs programs.claude-code.enable = true."
        "programs.xmsg.harnesses.agy.enable needs programs.antigravity-cli.enable = true."
      ];
    };

    # Oracle 3: darwin has the launchd agent and no systemd unit (evaluation only).
    # aarch64 only: nixpkgs 26.11 has dropped x86_64-darwin.
    test-darwin-aarch64 = darwinCells "aarch64-darwin";

    # Oracle 4: the NixOS module opens the port only with federation on.
    test-nixos-port = {
      expr = {
        on = lib.elem 7790 (nixosPorts true);
        off = lib.elem 7790 (nixosPorts false);
      };
      expected = {
        on = true;
        off = false;
      };
    };
  };
}
