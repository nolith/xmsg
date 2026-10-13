# homeManagerModules.xmsg: the `xmsg serve` user service and the per-harness pieces
# (MCP entry, skill, agy hook, pi extension).
#
# `packages` is this flake's `packages` output, the default for `programs.xmsg.package`.
{ packages }:
{
  config,
  lib,
  pkgs,
  ...
}:
let
  inherit (lib)
    mkEnableOption
    mkIf
    mkMerge
    mkOption
    types
    ;
  cfg = config.programs.xmsg;
  srv = cfg.server;
  fed = srv.federation;
  exe = lib.getExe cfg.package;
  skill = ./skills/xmsg;

  # The MCP server every MCP harness runs: `xmsg mcp`, a stdio server.
  mcpEntry = {
    command = exe;
    args = [ "mcp" ];
  };

  peerType = types.submodule {
    options = {
      address = mkOption {
        type = types.str;
        description = "The peer's federation endpoint, `host:port`.";
      };
      pin = mkOption {
        type = types.nullOr types.str;
        default = null;
        description = "The peer's SPKI pin, `sha256:<hex>`. Exactly one of `pin`, `pinFromCert` and `ca`.";
      };
      pinFromCert = mkOption {
        type = types.nullOr types.path;
        default = null;
        description = "A PEM certificate whose SPKI SHA-256 becomes the entry's `pin`, computed at build time.";
      };
      ca = mkOption {
        type = types.nullOr (types.either types.path types.str);
        default = null;
        description = "PEM trust anchor bundle. A path is copied to the store; a string is a runtime path. Needs `identities`.";
      };
      identities = mkOption {
        type = types.nullOr (types.listOf types.str);
        default = null;
        description = "The peer certificate's URI SAN (exactly one). Required with `ca`.";
      };
      allow = mkOption {
        type = types.listOf (
          types.enum [
            "send"
            "reply"
            "list"
          ]
        );
        default = [ ];
        description = "Operations the peer may perform here.";
      };
      from = mkOption {
        type = types.nullOr (types.listOf types.str);
        default = null;
        description = "Source CIDR blocks the peer may connect from.";
      };
      leaf = mkOption {
        type = types.bool;
        default = false;
        description = "The peer is a leaf: sends to it are queued for it to collect, never pushed.";
      };
      principals = mkOption {
        type = types.listOf types.str;
        default = [ ];
        description = "Sender filters: a service name, an instance ID, or `anon:<from>`.";
      };
      targets = mkOption {
        type = types.nullOr (types.listOf types.str);
        default = null;
        description = "Local targets the peer may reach: a service name or an instance ID. Null admits every target.";
      };
      dial = mkOption {
        type = types.nullOr (types.enum [ "outbound" ]);
        default = null;
        description = "`outbound`: this node dials the peer and holds the link open (README 7.6).";
      };
    };
  };

  # One entry in xmsg's peers-file schema (README 7.3): absent fields are omitted, as the
  # loader's defaults read them.
  renderPeer =
    _: p:
    lib.filterAttrs (_: v: v != null) {
      inherit (p)
        address
        pin
        identities
        allow
        from
        targets
        dial
        ;
      ca = if p.ca == null then null else "${p.ca}";
    }
    // lib.optionalAttrs p.leaf { leaf = true; }
    // lib.optionalAttrs (p.principals != [ ]) { inherit (p) principals; };

  peersJson = builtins.toJSON (lib.mapAttrs renderPeer fed.peers);
  pinned = lib.filterAttrs (_: p: p.pinFromCert != null) fed.peers;
  # The pin is the SHA-256 of the cert's DER SubjectPublicKeyInfo, as xmsg's
  # spki_sha256_from_der computes it (src/fed.rs).
  peersBase = pkgs.writeText "xmsg-peers.json" peersJson;
  peersFile =
    if pinned == { } then
      peersBase
    else
      pkgs.runCommand "xmsg-peers.json"
        {
          nativeBuildInputs = [
            pkgs.jq
            pkgs.openssl
          ];
        }
        ''
          cp ${peersBase} peers.json
          ${lib.concatStrings (
            lib.mapAttrsToList (name: p: ''
              openssl x509 -in ${p.pinFromCert} -pubkey -noout > pub.pem
              openssl pkey -pubin -in pub.pem -outform DER > spki.der
              pin=$(sha256sum spki.der | cut -d' ' -f1)
              jq --arg name ${lib.escapeShellArg name} --arg pin "sha256:$pin" '.[$name].pin = $pin' peers.json > next.json
              mv next.json peers.json
            '') pinned
          )}
          cp peers.json $out
        '';

  serveArgs =
    lib.optionals (srv.httpSocket != null) [
      "--http-sock"
      srv.httpSocket
    ]
    ++ lib.concatMap (p: [
      "--agy-exe"
      p
    ]) srv.agyExe
    ++ lib.concatLists (
      lib.mapAttrsToList (name: p: [
        "--svc-exe"
        "${name}=${p}"
      ]) srv.svcExe
    )
    ++ lib.optionals fed.enable (
      [
        "--fed-listen"
        "${fed.listenAddress}:${toString fed.port}"
        "--fed-cert"
        "${fed.cert}"
        "--fed-key"
        fed.key
        "--peers-file"
        "${peersFile}"
      ]
      ++ lib.optionals (fed.reloadInterval != null) [
        "--fed-reload-interval-secs"
        (toString fed.reloadInterval)
      ]
    );

  # A switch whose harness module is off would install nothing; refuse it, naming the option.
  requireHarness = switch: option: enabled: {
    assertion = enabled;
    message = "programs.xmsg.harnesses.${switch}.enable needs ${option} = true.";
  };
in
{
  options.programs.xmsg = {
    enable = mkEnableOption "xmsg, the bridge into running agent sessions";

    package = mkOption {
      type = types.package;
      default = packages.${pkgs.stdenv.hostPlatform.system}.default;
      defaultText = lib.literalExpression "xmsg.packages.\${system}.default";
      description = "The xmsg package.";
    };

    server = {
      enable = mkOption {
        type = types.bool;
        default = true;
        description = "Run `xmsg serve` as a user service (systemd on Linux, launchd on darwin).";
      };
      httpSocket = mkOption {
        type = types.nullOr types.str;
        default = null;
        description = "`--http-sock`. Null takes xmsg's default, `$XDG_RUNTIME_DIR/xmsg/http.sock`.";
      };
      agyExe = mkOption {
        type = types.listOf types.str;
        default = lib.optional (
          cfg.harnesses.agy.enable && config.programs.antigravity-cli.package != null
        ) "${config.programs.antigravity-cli.package}/bin/agy";
        defaultText = lib.literalExpression ''[ "''${config.programs.antigravity-cli.package}/bin/agy" ] when harnesses.agy is on'';
        description = "`--agy-exe`: the agy executables whose sessions xmsg attests (the identity anchor).";
      };
      svcExe = mkOption {
        type = types.attrsOf types.str;
        default = { };
        example = lib.literalExpression "{ my-bot = lib.getExe pkgs.my-bot; }";
        description = "`--svc-exe NAME=PATH`: the service names xmsg admits, each only from its executable.";
      };
      federation = {
        enable = mkEnableOption "cross-host federation (`--fed-listen`, `--fed-cert`, `--fed-key`, `--peers-file`)";
        listenAddress = mkOption {
          type = types.str;
          default = "0.0.0.0";
          description = "Address the federation listener binds.";
        };
        port = mkOption {
          type = types.port;
          default = 7788;
          description = "Federation listener port.";
        };
        cert = mkOption {
          type = types.either types.path types.str;
          description = "`--fed-cert`: this node's PEM certificate.";
        };
        key = mkOption {
          type = types.str;
          description = "`--fed-key`: this node's PEM private key, a runtime path (never a store path).";
        };
        reloadInterval = mkOption {
          type = types.nullOr types.ints.unsigned;
          default = null;
          description = "`--fed-reload-interval-secs`. Null takes xmsg's default (30).";
        };
        peers = mkOption {
          type = types.attrsOf peerType;
          default = { };
          description = "The peers file (`--peers-file`), keyed by node name or nickname (README 7.3).";
        };
      };
    };

    harnesses = {
      claude.enable = mkEnableOption "the xmsg MCP server, skill and tool permissions in Claude Code (`programs.claude-code`)";
      agy.enable = mkEnableOption "the xmsg MCP server, skill and PreInvocation hook in Antigravity (`programs.antigravity-cli`); the module owns `~/.gemini/config/hooks.json`";
      pi.enable = mkEnableOption "the xmsg pi extension and skill (`~/.pi/agent`)";
    };
  };

  config = mkIf cfg.enable (mkMerge [
    {
      home.packages = [ cfg.package ];

      assertions =
        lib.optional cfg.harnesses.claude.enable (
          requireHarness "claude" "programs.claude-code.enable" config.programs.claude-code.enable
        )
        ++ lib.optional cfg.harnesses.agy.enable (
          requireHarness "agy" "programs.antigravity-cli.enable" config.programs.antigravity-cli.enable
        )
        ++ lib.mapAttrsToList (name: p: {
          assertion =
            lib.count (x: x != null) [
              p.pin
              p.pinFromCert
              p.ca
            ] == 1;
          message = "programs.xmsg.server.federation.peers.${name}: set exactly one of pin, pinFromCert and ca.";
        }) fed.peers;
    }

    # Runs as the sessions' own user: the inbox sockets live in 0700 dirs. Its HTTP API is
    # a unix socket with the peer uid checked, and there is no TCP listener.
    (mkIf (srv.enable && pkgs.stdenv.hostPlatform.isLinux) {
      systemd.user.services.xmsg = {
        Unit.Description = "xmsg: bridge into running agent sessions";
        Service = {
          ExecStart = "${exe} serve ${lib.escapeShellArgs serveArgs}";
          Restart = "on-failure";
        };
        Install.WantedBy = [ "default.target" ];
      };
    })

    (mkIf (srv.enable && pkgs.stdenv.hostPlatform.isDarwin) {
      launchd.agents.xmsg = {
        enable = true;
        config = {
          ProgramArguments = [
            exe
            "serve"
          ]
          ++ serveArgs;
          KeepAlive.SuccessfulExit = false;
          RunAtLoad = true;
        };
      };
    })

    (mkIf cfg.harnesses.claude.enable {
      programs.claude-code = {
        mcpServers.xmsg = mcpEntry // {
          type = "stdio";
          env = { };
        };
        skills.xmsg = skill;
        # The tools are authorized explicitly: allowed calls skip the auto-mode classifier.
        # XH1b: add mcp__xmsg__replies with the new tool surface.
        settings.permissions.allow = [
          "mcp__xmsg__list"
          "mcp__xmsg__send"
          "mcp__xmsg__reply"
        ];
      };
    })

    (mkIf cfg.harnesses.agy.enable {
      programs.antigravity-cli = {
        mcpServers.xmsg = mcpEntry;
        skills.xmsg = skill;
      };
      # agy registers its language-server credentials before every turn. `--hook` reads them
      # from the hook's stdin JSON. programs.antigravity-cli has no hook option, so this
      # module owns the whole file.
      home.file.".gemini/config/hooks.json".text = builtins.toJSON {
        xmsg-register.PreInvocation = [
          {
            type = "command";
            command = "${exe} register agy --hook";
          }
        ];
      };
    })

    # pi auto-discovers extensions under ~/.pi/agent/extensions.
    (mkIf cfg.harnesses.pi.enable {
      home.file.".pi/agent/extensions/xmsg-pi.ts".source = ../../extensions/pi/index.ts;
      home.file.".pi/agent/skills/xmsg/SKILL.md".source = skill + "/SKILL.md";
    })
  ]);
}
