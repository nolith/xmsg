# Plan oracle 6: every peers file the typed `peers` option renders is accepted by THIS
# revision's loader (`xmsg check-peers`, the package this flake builds): the CA form, the pin
# form and a hub entry allowed to `forward`.
# Second property: the CA and pin forms equal what nix-config's renderer renders today
# (nix-config 174eb64c, modules/den/aspects/applications/dev/ai/mcp/xmsg.nix:352-397), for
# an equivalent input, compared after `jq -S`.
{ inputs, ... }:
{
  perSystem =
    {
      pkgs,
      lib,
      system,
      ...
    }:
    let
      inherit (inputs) xmsg home-manager;
      crt = ./fixtures/peer.crt;
      caCrt =
        c:
        builtins.path {
          path = c;
          name = builtins.unsafeDiscardStringContext (baseNameOf c);
        };

      # The module's rendering: the `--peers-file` argument of the evaluated unit.
      modulePeers =
        peers:
        let
          c =
            (home-manager.lib.homeManagerConfiguration {
              inherit pkgs;
              modules = [
                xmsg.homeManagerModules.xmsg
                {
                  home = {
                    username = "alice";
                    homeDirectory = "/home/alice";
                    stateVersion = "25.11";
                    enableNixpkgsReleaseCheck = false;
                  };
                  programs.xmsg = {
                    enable = true;
                    package = pkgs.hello;
                    server.federation = {
                      enable = true;
                      cert = "/run/secrets/xmsg.crt";
                      key = "/run/secrets/xmsg.key";
                      inherit peers;
                    };
                  };
                }
              ];
            }).config;
          argv = lib.splitString " " (lib.head c.systemd.user.services.xmsg.Service.ExecStart);
        in
        lib.elemAt argv (lib.lists.findFirstIndex (a: a == "--peers-file") null argv + 1);

      # nix-config's renderer, verbatim but for its owner bindings (host names, the
      # spiffe scheme, the domain), which are the fixture's here. A CA entry is keyed by the
      # node name its identity derives (README 7.0), so its key is `caNode`, not the host;
      # the pin entry keeps the host as its nickname.
      peerNode = "cortex";
      caNode = "alice.cortex";
      address = "cortex.ts.example.org:7788";
      spiffe = "spiffe://example.org/host/cortex/user/alice";
      leaves = [
        {
          name = "genie.axon";
          identity = "spiffe://example.org/ns/axon/sa/genie";
          allow = [ "reply" ];
          targets = [ "genie-guard" ];
        }
      ];
      refCa = pkgs.writeText "xmsg-peers.json" (
        builtins.toJSON (
          {
            ${caNode} = {
              inherit address;
              ca = "${caCrt crt}";
              identities = [ spiffe ];
              allow = [
                "send"
                "reply"
                "list"
              ];
            };
          }
          // lib.listToAttrs (
            map (
              leaf:
              lib.nameValuePair leaf.name {
                address = "${leaf.name}.invalid:7788";
                ca = "${caCrt crt}";
                identities = [ leaf.identity ];
                leaf = true;
                inherit (leaf) allow targets;
              }
            ) leaves
          )
        )
      );
      refPin =
        pkgs.runCommand "xmsg-peers.json"
          {
            nativeBuildInputs = [
              pkgs.jq
              pkgs.openssl
            ];
          }
          ''
            openssl x509 -in ${crt} -pubkey -noout > pub.pem
            openssl pkey -pubin -in pub.pem -outform DER > spki.der
            pin=$(sha256sum spki.der | cut -d' ' -f1)
            jq -n --arg name ${peerNode} --arg pin "sha256:$pin" \
              --arg address ${address} \
              '{($name): {address: $address, pin: $pin, allow: ["send", "reply", "list"]}}' > $out
          '';

      allowAll = [
        "send"
        "reply"
        "list"
      ];
      mineCa = modulePeers {
        ${caNode} = {
          inherit address;
          ca = crt;
          identities = [ spiffe ];
          allow = allowAll;
        };
        "genie.axon" = {
          address = "genie.axon.invalid:7788";
          ca = crt;
          identities = [ "spiffe://example.org/ns/axon/sa/genie" ];
          leaf = true;
          allow = [ "reply" ];
          targets = [ "genie-guard" ];
        };
      };
      minePin = modulePeers {
        cortex = {
          inherit address;
          pinFromCert = crt;
          allow = allowAll;
        };
      };
      # README 7.7: a hub's entry for a peer whose sends it relays (`forward`), and a
      # recipient's entry for that hub (`relayed`).
      mineHub = modulePeers {
        relayer = {
          address = "relayer.example.org:7788";
          pin = "sha256:1111111111111111111111111111111111111111111111111111111111111111";
          allow = [
            "send"
            "relayed"
          ];
        };
        hub = {
          address = "hub.example.org:7788";
          pin = "sha256:0000000000000000000000000000000000000000000000000000000000000000";
          allow = [
            "send"
            "forward"
          ];
        };
      };
      xmsgExe = lib.getExe xmsg.packages.${system}.default;
      opencodeCleanupScript =
        (home-manager.lib.homeManagerConfiguration {
          inherit pkgs;
          modules = [
            xmsg.homeManagerModules.xmsg
            {
              home = {
                username = "alice";
                homeDirectory = "/home/alice";
                stateVersion = "25.11";
                enableNixpkgsReleaseCheck = false;
              };
              programs.xmsg = {
                enable = true;
                package = pkgs.hello;
                harnesses.opencode.enable = true;
              };
            }
          ];
        }).config.home.activation.xmsgCleanupLegacyOpencodeSkill.data;
    in
    {
      checks.peers-render =
        pkgs.runCommand "xmsg-peers-render"
          {
            nativeBuildInputs = [
              pkgs.jq
              pkgs.diffutils
            ];
          }
          ''
            same() {
              jq -S . "$2" > mine.json
              jq -S . "$3" > ref.json
              if ! diff -u ref.json mine.json; then
                echo "peers-render: the $1 form differs from nix-config's renderer (- nix-config, + module)" >&2
                exit 1
              fi
              echo "peers-render: $1 form equal"
            }
            loads() {
              if ! ${xmsgExe} check-peers "$2"; then
                echo "peers-render: the $1 form is refused by this revision's loader" >&2
                exit 1
              fi
            }
            loads CA ${mineCa}
            loads pin ${minePin}
            loads hub ${mineHub}
            same CA ${mineCa} ${refCa}
            same pin ${minePin} ${refPin}

            # XH1.1 (1) oracle: legacy opencode skill cleanup removes a /nix/store symlink
            # and leaves a non-store directory or non-store symlink untouched.
            export HOME="$TMPDIR/home"
            unset XDG_CONFIG_HOME
            mkdir -p "$HOME/.config/opencode/skills"

            # Case 1: symlink into /nix/store -> removed
            ln -s "${pkgs.hello}" "$HOME/.config/opencode/skills/xmsg"
            ( ${opencodeCleanupScript} )
            if [ -e "$HOME/.config/opencode/skills/xmsg" ] || [ -L "$HOME/.config/opencode/skills/xmsg" ]; then
              echo "opencode-cleanup: expected /nix/store symlink to be removed" >&2
              exit 1
            fi

            # Case 2: real non-store directory -> left alone
            mkdir -p "$HOME/.config/opencode/skills/xmsg"
            touch "$HOME/.config/opencode/skills/xmsg/user-file.md"
            ( ${opencodeCleanupScript} )
            if [ ! -f "$HOME/.config/opencode/skills/xmsg/user-file.md" ]; then
              echo "opencode-cleanup: expected non-store directory and its contents to be preserved" >&2
              exit 1
            fi
            rm -rf "$HOME/.config/opencode/skills/xmsg"

            # Case 3: symlink to non-store path -> left alone
            mkdir -p "$TMPDIR/custom-skill"
            ln -s "$TMPDIR/custom-skill" "$HOME/.config/opencode/skills/xmsg"
            ( ${opencodeCleanupScript} )
            if [ ! -L "$HOME/.config/opencode/skills/xmsg" ]; then
              echo "opencode-cleanup: expected non-store symlink to be preserved" >&2
              exit 1
            fi

            touch $out
          '';
    };
}
